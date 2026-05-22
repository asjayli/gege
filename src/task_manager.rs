use crate::executors::factory;
use crate::pipeline::{TaskRequest, TaskResponse, TaskStatus, TaskStatusResponse};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tonic::Status;
use log::{error, info};

/// 已完成的任务在内存中保留的时长（供状态查询）
const TASK_RETENTION_TTL_SECS: u64 = 3600;
/// 清理扫描间隔
const CLEANUP_INTERVAL_SECS: u64 = 300;
/// 默认最大并发任务数
const DEFAULT_MAX_CONCURRENT: usize = 100;
/// 回调最大重试次数
const CALLBACK_MAX_RETRIES: u32 = 3;
/// 回调重试基础延迟（秒）
const CALLBACK_RETRY_BASE_DELAY_SECS: u64 = 2;

/// TaskStatus 枚举值转可读字符串（用于回调 JSON 序列化）
pub fn task_status_to_string(status: i32) -> &'static str {
    match TaskStatus::try_from(status) {
        Ok(TaskStatus::Running) => "RUNNING",
        Ok(TaskStatus::Completed) => "COMPLETED",
        Ok(TaskStatus::Failed) => "FAILED",
        Ok(TaskStatus::Timeout) => "TIMEOUT",
        Ok(TaskStatus::Cancelled) => "CANCELLED",
        _ => "UNKNOWN",
    }
}

/// 将 Gege 回调 enrich 为 xixi AgentTaskCallbackDto 兼容格式。
fn enrich_callback_payload(mut payload: serde_json::Value, metadata_json: &str) -> serde_json::Value {
    if let Some(obj) = payload.as_object_mut() {
        let output = obj
            .get("finalResult")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| obj.get("agentRaw").and_then(|v| v.as_str()))
            .unwrap_or("");
        obj.insert(
            "outputData".to_string(),
            serde_json::Value::String(output.to_string()),
        );
        if !metadata_json.is_empty() {
            obj.insert(
                "metadataJson".to_string(),
                serde_json::Value::String(metadata_json.to_string()),
            );
            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(metadata_json) {
                if let Some(meta_obj) = meta.as_object() {
                    copy_meta_field(obj, meta_obj, "flowInstanceId", "flowInstId");
                    copy_meta_field(obj, meta_obj, "flowInstId", "flowInstId");
                    copy_meta_field(obj, meta_obj, "lockId", "lockId");
                    copy_meta_field(obj, meta_obj, "outputVarKey", "outputVarKey");
                    copy_meta_field(obj, meta_obj, "employeeId", "employeeId");
                    copy_meta_field(obj, meta_obj, "agentCode", "employeeId");
                    copy_meta_field(obj, meta_obj, "agentRunId", "agentRunId");
                    copy_meta_field(obj, meta_obj, "equipmentLoadoutJson", "equipmentLoadoutJson");
                    copy_meta_field(obj, meta_obj, "taskType", "taskType");
                    copy_meta_field(obj, meta_obj, "smartPlanFlowCode", "smartPlanFlowCode");
                    copy_meta_field(obj, meta_obj, "publishUrl", "publishUrl");
                }
            }
        }
    }
    payload
}

fn copy_meta_field(
    target: &mut serde_json::Map<String, serde_json::Value>,
    source: &serde_json::Map<String, serde_json::Value>,
    source_key: &str,
    target_key: &str,
) {
    if target.contains_key(target_key) {
        return;
    }
    if let Some(value) = source.get(source_key) {
        target.insert(target_key.to_string(), value.clone());
    }
}

pub(crate) struct TaskEntry {
    pub(crate) handle: Option<JoinHandle<()>>,
    pub(crate) status: i32,
    pub(crate) message: String,
    pub(crate) completed_at: Option<Instant>,
}

pub struct TaskManager {
    pub(crate) tasks: Arc<Mutex<HashMap<String, TaskEntry>>>,
    client: Arc<reqwest::Client>,
    max_concurrent: usize,
    running_count: Arc<AtomicUsize>,
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskManager {
    pub fn new() -> Self {
        let tasks: Arc<Mutex<HashMap<String, TaskEntry>>> = Arc::new(Mutex::new(HashMap::new()));
        let client = Arc::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        );

        // 启动后台清理任务，定期移除已完成的旧任务
        let cleanup_tasks = tasks.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
            loop {
                interval.tick().await;
                let mut map = cleanup_tasks.lock().await;
                let now = Instant::now();
                let ttl = Duration::from_secs(TASK_RETENTION_TTL_SECS);
                map.retain(|_id, entry| match entry.completed_at {
                    Some(t) => now.duration_since(t) < ttl,
                    None => true,
                });
            }
        });

        let running_count = Arc::new(AtomicUsize::new(0));

        Self { tasks, client, max_concurrent: DEFAULT_MAX_CONCURRENT, running_count }
    }

    pub async fn get_task_status(&self, task_id: &str) -> TaskStatusResponse {
        let map = self.tasks.lock().await;
        match map.get(task_id) {
            Some(entry) => TaskStatusResponse {
                exists: true,
                status: entry.status,
                message: entry.message.clone(),
            },
            None => TaskStatusResponse {
                exists: false,
                status: TaskStatus::Cancelled as i32,
                message: "Task not found. It may have expired or never existed.".to_string(),
            },
        }
    }

    pub async fn start_task(
        &self,
        req: TaskRequest,
        tx: Option<mpsc::Sender<Result<TaskResponse, Status>>>,
    ) -> Result<(), anyhow::Error> {
        let task_id = req.task_id.clone();

        // 校验 task_id 不为空
        if task_id.trim().is_empty() {
            return Err(anyhow::anyhow!("task_id must not be empty"));
        }

        let tasks_map_inner = self.tasks.clone();
        let task_id_spawned = task_id.clone();
        let task_id_update = task_id.clone();
        let client = self.client.clone();
        let max_concurrent = self.max_concurrent;
        let running_count = self.running_count.clone();

        let executor = factory::create_executor(&req.agent_type());

        // 原子性检查重复 + 并发上限 + 插入，消除 TOCTOU 竞态
        {
            let mut map = self.tasks.lock().await;

            // 并发上限检查（O(1)）
            let current_running = self.running_count.load(Ordering::SeqCst);
            if current_running >= max_concurrent {
                return Err(anyhow::anyhow!(
                    "max concurrent tasks ({}) reached",
                    max_concurrent
                ));
            }

            // 重复 ID 检查
            if let Some(entry) = map.get(&task_id) {
                if entry.handle.is_some() || entry.status == TaskStatus::Running as i32 {
                    return Err(anyhow::anyhow!(
                        "task_id '{}' is already running",
                        task_id
                    ));
                }
            }

            // 插入占位
            map.insert(
                task_id.clone(),
                TaskEntry {
                    handle: None,
                    status: TaskStatus::Running as i32,
                    message: "Task is initializing.".to_string(),
                    completed_at: None,
                },
            );
            
            // 在锁内增加 running_count，避免与任务完成时的 fetch_sub 发生竞态
            self.running_count.fetch_add(1, Ordering::SeqCst);
        }

        let handle = tokio::spawn(async move {
            info!("Starting execution for task {}", task_id_spawned);

            let (inner_tx, mut internal_rx) = mpsc::channel(128);

            let stream_tx = tx.clone();
            let callback_url = req.callback_url.clone();
            let callback_headers = req.callback_headers.clone();
            let callback_format = req.callback_format;
            let metadata_json = req.metadata_json.clone();

            let exec_handle = tokio::spawn(async move {
                executor.execute(&req, inner_tx).await;
            });
            
            // 确保外层 task 被 abort 时，内层 executor 也会被 abort
            struct AbortOnDrop(tokio::task::JoinHandle<()>);
            impl Drop for AbortOnDrop {
                fn drop(&mut self) {
                    self.0.abort();
                }
            }
            let _abort_guard = AbortOnDrop(exec_handle);

            let mut last_status = TaskStatus::Running as i32;

            while let Some(res) = internal_rx.recv().await {
                if let Some(ref grpc_tx) = stream_tx {
                    let _ = grpc_tx.send(res.clone()).await;
                }

                match res {
                    Ok(task_res) => {
                        last_status = task_res.status;

                        // 仅在任务结束时（非 RUNNING 状态）发送回调，避免每行日志触发一次 HTTP 请求导致风暴
                        if !callback_url.is_empty() && task_res.status != TaskStatus::Running as i32 {
                            let format_type =
                                crate::pipeline::CallbackFormat::try_from(callback_format)
                                    .unwrap_or(crate::pipeline::CallbackFormat::Default);

                            let payload = match format_type {
                                crate::pipeline::CallbackFormat::FeishuBot => serde_json::json!({
                                    "msg_type": "text",
                                    "content": {
                                        "text": format!("[Task {} - Status: {}]\n{}", task_res.task_id, task_status_to_string(task_res.status), if task_res.final_result.is_empty() { &task_res.agent_raw } else { &task_res.final_result })
                                    }
                                }),
                                crate::pipeline::CallbackFormat::WecomBot => serde_json::json!({
                                    "msgtype": "text",
                                    "text": {
                                        "content": format!("[Task {} - Status: {}]\n{}", task_res.task_id, task_status_to_string(task_res.status), if task_res.final_result.is_empty() { &task_res.agent_raw } else { &task_res.final_result })
                                    }
                                }),
                                _ => enrich_callback_payload(serde_json::json!({
                                    "taskId": task_res.task_id,
                                    "status": task_status_to_string(task_res.status),
                                    "logChunk": task_res.log_chunk,
                                    "finalResult": task_res.final_result,
                                    "agentText": task_res.agent_text,
                                    "agentRaw": task_res.agent_raw,
                                    "sessionId": task_res.session_id,
                                    "parsed": task_res.parsed
                                }), &metadata_json),
                            };

                            // 异步发送回调（含重试），避免阻塞 internal_rx 消费循环
                            let cb_url = callback_url.clone();
                            let cb_headers = callback_headers.clone();
                            let cb_task_id = task_res.task_id.clone();
                            let cb_client = client.clone();
                            tokio::spawn(async move {
                                for attempt in 0..=CALLBACK_MAX_RETRIES {
                                    let mut request_builder = cb_client.post(&cb_url);
                                    for (k, v) in &cb_headers {
                                        request_builder = request_builder.header(k, v);
                                    }

                                    match request_builder.json(&payload).send().await {
                                        Ok(r) if r.status().is_success() => break,
                                        Ok(r) => {
                                            error!(
                                                "Callback failed for task {} with status {} (attempt {}/{})",
                                                cb_task_id, r.status(), attempt + 1, CALLBACK_MAX_RETRIES + 1
                                            );
                                        }
                                        Err(e) => {
                                            error!(
                                                "Callback network error for task {}: {} (attempt {}/{})",
                                                cb_task_id, e, attempt + 1, CALLBACK_MAX_RETRIES + 1
                                            );
                                        }
                                    }

                                    if attempt < CALLBACK_MAX_RETRIES {
                                        tokio::time::sleep(Duration::from_secs(
                                            CALLBACK_RETRY_BASE_DELAY_SECS * (attempt as u64 + 1),
                                        ))
                                        .await;
                                    }
                                }
                            });
                        }
                    }
                    Err(e) => {
                        last_status = TaskStatus::Failed as i32;
                        error!("Executor returned error for task {}: {}", task_id_spawned, e);
                    }
                }
            }

            // 更新任务状态（保留在 map 中供查询，标记完成时间供 TTL 清理）
            let mut map = tasks_map_inner.lock().await;
            if let Some(entry) = map.get_mut(&task_id_update) {
                entry.handle = None;
                entry.status = last_status;
                entry.message = format!("Task finished with status: {}", task_status_to_string(last_status));
                entry.completed_at = Some(Instant::now());
            }
            running_count.fetch_sub(1, Ordering::SeqCst);
            info!("Task {} finished", task_id_update);
        });

        // 更新占位条目为实际的 JoinHandle
        {
            let mut map = self.tasks.lock().await;
            if let Some(entry) = map.get_mut(&task_id) {
                if entry.status == TaskStatus::Running as i32 {
                    entry.handle = Some(handle);
                    entry.message = "Task is currently running.".to_string();
                } else {
                    handle.abort();
                }
            } else {
                handle.abort();
            }
        }

        Ok(())
    }

    pub async fn cancel_task(&self, task_id: &str) -> bool {
        let mut map = self.tasks.lock().await;
        if let Some(entry) = map.get_mut(task_id) {
            if entry.status == TaskStatus::Running as i32 {
                if let Some(handle) = entry.handle.take() {
                    handle.abort();
                }
                self.running_count.fetch_sub(1, Ordering::SeqCst);
                entry.status = TaskStatus::Cancelled as i32;
                entry.message = "Task was cancelled.".to_string();
                entry.completed_at = Some(Instant::now());
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::AgentType;

    #[tokio::test]
    async fn test_task_manager_new() {
        let manager = TaskManager::new();
        assert!(manager.tasks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_task_manager_empty_id_rejected() {
        let manager = TaskManager::new();
        let req = TaskRequest {
            task_id: "  ".to_string(),
            agent_type: AgentType::Unknown as i32,
            prompt: "test".to_string(),
            workspace_dir: "".to_string(),
            env_vars: Default::default(),
            timeout_seconds: 3600,
            auth_token: "token".to_string(),
            callback_url: "".to_string(),
            callback_headers: Default::default(),
            callback_format: 0,
            metadata_json: String::new(),
        };

        let result = manager.start_task(req, None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must not be empty"));
    }

    #[tokio::test]
    async fn test_task_manager_duplicate_id_rejected() {
        let manager = TaskManager::new();

        // 先插入一个长时间运行的任务
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        });
        manager.tasks.lock().await.insert(
            "running-task".to_string(),
            TaskEntry {
                handle: Some(handle),
                status: TaskStatus::Running as i32,
                message: "Running".to_string(),
                completed_at: None,
            },
        );

        let req = TaskRequest {
            task_id: "running-task".to_string(),
            agent_type: AgentType::Unknown as i32,
            prompt: "test".to_string(),
            workspace_dir: "".to_string(),
            env_vars: Default::default(),
            timeout_seconds: 3600,
            auth_token: "token".to_string(),
            callback_url: "".to_string(),
            callback_headers: Default::default(),
            callback_format: 0,
            metadata_json: String::new(),
        };

        let result = manager.start_task(req, None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already running"));
    }

    #[tokio::test]
    async fn test_task_manager_start_and_status() {
        let manager = TaskManager::new();
        let req = TaskRequest {
            task_id: "test-task-1".to_string(),
            agent_type: AgentType::Unknown as i32,
            prompt: "test".to_string(),
            workspace_dir: "".to_string(),
            env_vars: Default::default(),
            timeout_seconds: 3600,
            auth_token: "token".to_string(),
            callback_url: "".to_string(),
            callback_headers: Default::default(),
            callback_format: 0,
            metadata_json: String::new(),
        };

        let result = manager.start_task(req, None).await;
        assert!(result.is_ok());

        let status = manager.get_task_status("test-task-1").await;
        assert!(!status.message.is_empty());
    }

    #[tokio::test]
    async fn test_task_manager_cancel() {
        let manager = TaskManager::new();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        });

        manager.tasks.lock().await.insert(
            "dummy-task".to_string(),
            TaskEntry {
                handle: Some(handle),
                status: TaskStatus::Running as i32,
                message: "Running".to_string(),
                completed_at: None,
            },
        );

        let cancelled = manager.cancel_task("dummy-task").await;
        assert!(cancelled);

        // 取消后状态应为 CANCELLED
        let status = manager.get_task_status("dummy-task").await;
        assert!(status.exists);
        assert_eq!(status.status, TaskStatus::Cancelled as i32);

        // 再次取消应返回 false（handle 已为 None）
        let cancelled_again = manager.cancel_task("dummy-task").await;
        assert!(!cancelled_again);
    }

    #[tokio::test]
    async fn test_task_manager_not_found() {
        let manager = TaskManager::new();
        let status = manager.get_task_status("nonexistent").await;
        assert!(!status.exists);
    }

    #[test]
    fn test_task_status_to_string() {
        assert_eq!(task_status_to_string(TaskStatus::Running as i32), "RUNNING");
        assert_eq!(
            task_status_to_string(TaskStatus::Completed as i32),
            "COMPLETED"
        );
        assert_eq!(task_status_to_string(TaskStatus::Failed as i32), "FAILED");
        assert_eq!(
            task_status_to_string(TaskStatus::Timeout as i32),
            "TIMEOUT"
        );
        assert_eq!(
            task_status_to_string(TaskStatus::Cancelled as i32),
            "CANCELLED"
        );
        assert_eq!(task_status_to_string(999), "UNKNOWN");
    }

    #[tokio::test]
    async fn test_task_manager_concurrency_limit() {
        let mut manager = TaskManager::new();
        manager.max_concurrent = 2;
        manager.running_count.store(2, std::sync::atomic::Ordering::SeqCst);

        let mut req = crate::pipeline::TaskRequest::default();
        req.task_id = "t3".to_string();

        let result = manager.start_task(req, None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max concurrent tasks"));
    }

    #[tokio::test]
    async fn test_task_manager_fast_task_running_count() {
        let manager = TaskManager::new();
        let mut req = crate::pipeline::TaskRequest::default();
        req.task_id = "fast".to_string();
        req.agent_type = AgentType::Unknown as i32; // UnknownExecutor 会立即退出

        assert!(manager.start_task(req, None).await.is_ok());
        
        // 等待异步任务执行完毕
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        // 验证 running_count 是否正确归零，没有发生泄漏
        assert_eq!(manager.running_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
