use crate::executors::factory;
use crate::pipeline::{TaskRequest, TaskResponse, TaskStatus, TaskStatusResponse};
use std::collections::HashMap;
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

struct TaskEntry {
    handle: Option<JoinHandle<()>>,
    status: i32,
    message: String,
    completed_at: Option<Instant>,
}

pub struct TaskManager {
    tasks: Arc<Mutex<HashMap<String, TaskEntry>>>,
    client: Arc<reqwest::Client>,
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskManager {
    pub fn new() -> Self {
        let tasks: Arc<Mutex<HashMap<String, TaskEntry>>> = Arc::new(Mutex::new(HashMap::new()));
        let client = Arc::new(reqwest::Client::new());

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

        Self { tasks, client }
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

        // 校验 task_id 不与正在运行的任务冲突
        {
            let map = self.tasks.lock().await;
            if let Some(entry) = map.get(&task_id) {
                if entry.handle.is_some() {
                    return Err(anyhow::anyhow!(
                        "task_id '{}' is already running",
                        task_id
                    ));
                }
            }
        }

        let tasks_map = self.tasks.clone();
        let task_id_for_map = task_id.clone();
        let task_id_for_entry = task_id.clone();
        let client = self.client.clone();

        let executor = factory::create_executor(&req.agent_type());

        let handle = tokio::spawn(async move {
            info!("Starting execution for task {}", task_id);

            let (inner_tx, mut internal_rx) = mpsc::channel(128);

            let stream_tx = tx.clone();
            let callback_url = req.callback_url.clone();
            let callback_headers = req.callback_headers.clone();
            let callback_format = req.callback_format;

            let exec_handle = tokio::spawn(async move {
                executor.execute(&req, inner_tx).await;
            });

            let mut last_status = TaskStatus::Running as i32;

            while let Some(res) = internal_rx.recv().await {
                if let Some(ref grpc_tx) = stream_tx {
                    let _ = grpc_tx.send(res.clone()).await;
                }

                match res {
                    Ok(task_res) => {
                        last_status = task_res.status;

                        if !callback_url.is_empty() {
                            let mut request_builder = client.post(&callback_url);

                            for (k, v) in &callback_headers {
                                request_builder = request_builder.header(k, v);
                            }

                            let format_type =
                                crate::pipeline::CallbackFormat::try_from(callback_format)
                                    .unwrap_or(crate::pipeline::CallbackFormat::Default);

                            let payload = match format_type {
                                crate::pipeline::CallbackFormat::FeishuBot => serde_json::json!({
                                    "msg_type": "text",
                                    "content": {
                                        "text": format!("[Task {} - Status: {}]\n{}", task_res.task_id, task_status_to_string(task_res.status), task_res.log_chunk)
                                    }
                                }),
                                crate::pipeline::CallbackFormat::WecomBot => serde_json::json!({
                                    "msgtype": "text",
                                    "text": {
                                        "content": format!("[Task {} - Status: {}]\n{}", task_res.task_id, task_status_to_string(task_res.status), task_res.log_chunk)
                                    }
                                }),
                                _ => serde_json::json!({
                                    "taskId": task_res.task_id,
                                    "status": task_status_to_string(task_res.status),
                                    "logChunk": task_res.log_chunk,
                                    "finalResult": task_res.final_result
                                }),
                            };

                            // 异步发送回调，避免阻塞 internal_rx 消费循环
                            let task_id_cb = task_res.task_id.clone();
                            tokio::spawn(async move {
                                match request_builder.json(&payload).send().await {
                                    Ok(r) if !r.status().is_success() => {
                                        error!(
                                            "Callback failed for task {} with status {}",
                                            task_id_cb,
                                            r.status()
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "Callback network error for task {}: {}",
                                            task_id_cb, e
                                        );
                                    }
                                    _ => {}
                                }
                            });
                        }
                    }
                    Err(e) => {
                        last_status = TaskStatus::Failed as i32;
                        error!("Executor returned error for task {}: {}", task_id, e);
                    }
                }
            }

            let _ = exec_handle.await;

            // 更新任务状态（保留在 map 中供查询，标记完成时间供 TTL 清理）
            let mut map = tasks_map.lock().await;
            if let Some(entry) = map.get_mut(&task_id_for_map) {
                entry.handle = None;
                entry.status = last_status;
                entry.message = format!("Task finished with status: {}", task_status_to_string(last_status));
                entry.completed_at = Some(Instant::now());
            }
            info!("Task {} finished", task_id_for_map);
        });

        // 插入时记录初始状态
        self.tasks.lock().await.insert(
            task_id_for_entry,
            TaskEntry {
                handle: Some(handle),
                status: TaskStatus::Running as i32,
                message: "Task is currently running.".to_string(),
                completed_at: None,
            },
        );
        Ok(())
    }

    pub async fn cancel_task(&self, task_id: &str) -> bool {
        let mut map = self.tasks.lock().await;
        if let Some(entry) = map.get_mut(task_id) {
            if let Some(handle) = entry.handle.take() {
                handle.abort();
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
}
