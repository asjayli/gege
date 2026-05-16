use crate::pipeline::{TaskRequest, TaskResponse, TaskStatus};
use async_trait::async_trait;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tonic::Status;
use log::{error, info};

pub mod factory;
pub mod claude_exe;
pub mod gemini_exe;
pub mod hehe_exe;
pub mod codex_exe;
pub mod opencode_exe;

#[async_trait]
pub trait AgentExecutor: Send + Sync {
    async fn execute(&self, req: &TaskRequest, tx: mpsc::Sender<Result<TaskResponse, Status>>);
}

// 提取一个通用的执行命令行任务的辅助函数
pub async fn execute_command(
    mut cmd: Command,
    req: &TaskRequest,
    tx: mpsc::Sender<Result<TaskResponse, Status>>,
) {
    if !req.workspace_dir.is_empty() {
        cmd.current_dir(&req.workspace_dir);
    }

    for (k, v) in &req.env_vars {
        cmd.env(k, v);
    }

    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            error!("Failed to spawn process: {}", e);
            let _ = tx.send(Ok(TaskResponse {
                task_id: req.task_id.clone(),
                status: TaskStatus::Failed as i32,
                log_chunk: format!("Failed to spawn process: {}", e),
                final_result: "".to_string(),
            })).await;
            return;
        }
    };

    let stdout = child.stdout.take().expect("Failed to open stdout");
    let stderr = child.stderr.take().expect("Failed to open stderr");

    let mut stdout_reader = BufReader::new(stdout).lines();
    let mut stderr_reader = BufReader::new(stderr).lines();

    let tx_stdout = tx.clone();
    let task_id_stdout = req.task_id.clone();
    let stdout_handle = tokio::spawn(async move {
        while let Ok(Some(line)) = stdout_reader.next_line().await {
            let _ = tx_stdout.send(Ok(TaskResponse {
                task_id: task_id_stdout.clone(),
                status: TaskStatus::Running as i32,
                log_chunk: format!("OUT: {}\n", line),
                final_result: "".to_string(),
            })).await;
        }
    });

    let tx_stderr = tx.clone();
    let task_id_stderr = req.task_id.clone();
    let stderr_handle = tokio::spawn(async move {
        while let Ok(Some(line)) = stderr_reader.next_line().await {
            let _ = tx_stderr.send(Ok(TaskResponse {
                task_id: task_id_stderr.clone(),
                status: TaskStatus::Running as i32,
                log_chunk: format!("ERR: {}\n", line),
                final_result: "".to_string(),
            })).await;
        }
    });

    let _ = tokio::join!(stdout_handle, stderr_handle);

    let timeout_duration = if req.timeout_seconds > 0 {
        std::time::Duration::from_secs(req.timeout_seconds as u64)
    } else {
        std::time::Duration::from_secs(86400 * 365) // No timeout (1 year)
    };

    match tokio::time::timeout(timeout_duration, child.wait()).await {
        Ok(Ok(status)) if status.success() => {
            info!("Task {} finished successfully", req.task_id);
            let _ = tx.send(Ok(TaskResponse {
                task_id: req.task_id.clone(),
                status: TaskStatus::Completed as i32,
                log_chunk: "Process finished successfully.\n".to_string(),
                final_result: "Success".to_string(),
            })).await;
        }
        Ok(Ok(status)) => {
            error!("Task {} failed with status {}", req.task_id, status);
            let _ = tx.send(Ok(TaskResponse {
                task_id: req.task_id.clone(),
                status: TaskStatus::Failed as i32,
                log_chunk: format!("Process finished with error status: {}\n", status),
                final_result: "".to_string(),
            })).await;
        }
        Ok(Err(e)) => {
            error!("Task {} process wait error: {}", req.task_id, e);
            let _ = tx.send(Ok(TaskResponse {
                task_id: req.task_id.clone(),
                status: TaskStatus::Failed as i32,
                log_chunk: format!("Wait process error: {}\n", e),
                final_result: "".to_string(),
            })).await;
        }
        Err(_) => {
            error!("Task {} timed out after {}s", req.task_id, req.timeout_seconds);
            let _ = child.kill().await;
            let _ = tx.send(Ok(TaskResponse {
                task_id: req.task_id.clone(),
                status: TaskStatus::Timeout as i32,
                log_chunk: format!("Process timed out and was killed after {}s.\n", req.timeout_seconds),
                final_result: "".to_string(),
            })).await;
        }
    }
}
