use crate::executors::factory;
use crate::pipeline::{TaskRequest, TaskResponse, TaskStatus, TaskStatusResponse};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tonic::Status;
use log::{info, error};

pub struct TaskManager {
    tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_task_status(&self, task_id: &str) -> TaskStatusResponse {
        let map = self.tasks.lock().await;
        let exists = map.contains_key(task_id);
        
        TaskStatusResponse {
            exists,
            status: if exists { TaskStatus::Running as i32 } else { TaskStatus::Completed as i32 }, // 如果不在内存中且被查问，通常代表刚完成或不存在
            message: if exists {
                "Task is currently running in the background.".to_string()
            } else {
                "Task not found. It may have completed, failed, or never existed.".to_string()
            },
        }
    }

    pub async fn start_task(
        &self,
        req: TaskRequest,
        tx: Option<mpsc::Sender<Result<TaskResponse, Status>>>,
    ) -> Result<(), anyhow::Error> {
        let task_id = req.task_id.clone();
        let tasks_map = self.tasks.clone();
        let task_id_for_map = task_id.clone();
        let task_id_for_insert = task_id.clone();

        let executor = factory::create_executor(&req.agent_type());
        
        let handle = tokio::spawn(async move {
            info!("Starting execution for task {}", task_id);
            
            // 如果tx是None，我们需要创建一个内部通道来接收日志，并通过HTTP回调给Java
            let (inner_tx, mut internal_rx) = mpsc::channel(128);
            
            // 复制一个发送者，如果是Stream模式则发送给Java Stream，否则静默丢弃（或者你可以将部分日志写本地文件）
            let stream_tx = tx.clone();
            let callback_url = req.callback_url.clone();
            let callback_headers = req.callback_headers.clone();
            let callback_format = req.callback_format;

            // 在另一个异步任务中运行执行器
            let exec_handle = tokio::spawn(async move {
                executor.execute(&req, inner_tx).await;
            });

            // 监听结果的循环
            let client = reqwest::Client::new();
            
            while let Some(res) = internal_rx.recv().await {
                // 如果是Stream模式，将结果发给gRPC流
                if let Some(ref grpc_tx) = stream_tx {
                    let _ = grpc_tx.send(res.clone()).await;
                }

                // 取出实际的Response对象
                if let Ok(task_res) = res {
                    // 如果有设置回调URL，发送回调
                    if !callback_url.is_empty() {
                        let mut request_builder = client.post(&callback_url);
                        
                        // 1. 注入用户自定义的 Headers (例如 Java 的 token、飞书/微信的签名校验头)
                        for (k, v) in &callback_headers {
                            request_builder = request_builder.header(k, v);
                        }

                        // 2. 根据格式类型适配 Payload
                        let format_type = crate::pipeline::CallbackFormat::try_from(callback_format).unwrap_or(crate::pipeline::CallbackFormat::Default);
                        
                        let payload = match format_type {
                            crate::pipeline::CallbackFormat::FeishuBot => {
                                // 飞书群机器人的 Webhook 格式
                                serde_json::json!({
                                    "msg_type": "text",
                                    "content": {
                                        "text": format!("[Task {} - Status: {}]\n{}", task_res.task_id, task_res.status, task_res.log_chunk)
                                    }
                                })
                            },
                            crate::pipeline::CallbackFormat::WecomBot => {
                                // 企业微信群机器人的 Webhook 格式
                                serde_json::json!({
                                    "msgtype": "text",
                                    "text": {
                                        "content": format!("[Task {} - Status: {}]\n{}", task_res.task_id, task_res.status, task_res.log_chunk)
                                    }
                                })
                            },
                            _ => {
                                // Gege 默认内部格式 (Java 接收用)
                                serde_json::json!({
                                    "taskId": task_res.task_id,
                                    "status": task_res.status,
                                    "logChunk": task_res.log_chunk,
                                    "finalResult": task_res.final_result
                                })
                            }
                        };
                        
                        // 异步非阻塞发送
                        match request_builder.json(&payload).send().await {
                            Ok(r) if !r.status().is_success() => {
                                error!("Callback failed for task {} with status {}", task_res.task_id, r.status());
                            }
                            Err(e) => {
                                error!("Callback network error for task {}: {}", task_res.task_id, e);
                            }
                            _ => {}
                        }
                    }
                }
            }
            
            let _ = exec_handle.await;

            // Remove from map when done
            let mut map = tasks_map.lock().await;
            map.remove(&task_id_for_map);
            info!("Task {} finished and removed from manager", task_id_for_map);
        });

        self.tasks.lock().await.insert(task_id_for_insert, handle);
        Ok(())
    }

    pub async fn cancel_task(&self, task_id: &str) -> bool {
        let mut map = self.tasks.lock().await;
        if let Some(handle) = map.remove(task_id) {
            handle.abort();
            true
        } else {
            false
        }
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
    async fn test_task_manager_start_and_status() {
        let manager = TaskManager::new();
        let req = TaskRequest {
            task_id: "test-task-1".to_string(),
            agent_type: AgentType::Unknown as i32, // Unknown executor finishes immediately
            prompt: "test".to_string(),
            workspace_dir: "".to_string(),
            env_vars: Default::default(),
            timeout_seconds: 3600,
            auth_token: "token".to_string(),
            callback_url: "".to_string(),
            callback_headers: Default::default(),
            callback_format: 0,
        };

        // start task
        let result = manager.start_task(req, None).await;
        assert!(result.is_ok());

        // Since it's UnknownExecutor, it might finish very quickly.
        // We can just verify get_task_status doesn't panic.
        let status = manager.get_task_status("test-task-1").await;
        // It might be true or false depending on execution speed, but we can verify message
        assert!(!status.message.is_empty());
    }

    #[tokio::test]
    async fn test_task_manager_cancel() {
        let manager = TaskManager::new();
        // Since we want to test cancel, we need a task that doesn't finish immediately,
        // but we only have UnknownExecutor which is fast.
        // We'll insert a dummy task into the map directly to test the cancellation logic.
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        });
        
        manager.tasks.lock().await.insert("dummy-task".to_string(), handle);

        // Cancel it
        let cancelled = manager.cancel_task("dummy-task").await;
        assert!(cancelled);

        // Cancel again should return false
        let cancelled_again = manager.cancel_task("dummy-task").await;
        assert!(!cancelled_again);

        let status = manager.get_task_status("dummy-task").await;
        assert!(!status.exists);
    }
}
