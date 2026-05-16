use crate::config::DANGEROUS_ENV_VARS;
use crate::pipeline::{TaskRequest, TaskResponse, TaskStatus};
use async_trait::async_trait;
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tonic::Status;
use log::{error, info};

pub mod claude_exe;
pub mod codex_exe;
pub mod factory;
pub mod gemini_exe;
pub mod hehe_exe;
pub mod opencode_exe;

/// 当 timeout_seconds 为 0 时的默认超时值（等同于无限制）
const DEFAULT_TIMEOUT_SECS: u64 = 86400 * 365 * 10;

#[async_trait]
pub trait AgentExecutor: Send + Sync {
    async fn execute(&self, req: &TaskRequest, tx: mpsc::Sender<Result<TaskResponse, Status>>);
}

/// 提取一个通用的执行命令行任务的辅助函数
pub async fn execute_command(
    mut cmd: Command,
    req: &TaskRequest,
    tx: mpsc::Sender<Result<TaskResponse, Status>>,
) {
    // 校验并设置工作目录
    if !req.workspace_dir.is_empty() {
        let workspace = Path::new(&req.workspace_dir);
        if !workspace.exists() || !workspace.is_dir() {
            let msg = format!(
                "Invalid workspace_dir: '{}' does not exist or is not a directory",
                req.workspace_dir
            );
            error!("{}", msg);
            let _ = tx
                .send(Ok(TaskResponse {
                    task_id: req.task_id.clone(),
                    status: TaskStatus::Failed as i32,
                    log_chunk: msg,
                    final_result: String::new(),
                }))
                .await;
            return;
        }
        cmd.current_dir(workspace);
    }

    // 过滤危险环境变量
    for (k, v) in &req.env_vars {
        if DANGEROUS_ENV_VARS.contains(&k.as_str()) {
            info!(
                "Blocked dangerous env var injection: {} for task {}",
                k, req.task_id
            );
            continue;
        }
        cmd.env(k, v);
    }

    // 进程被 drop 时自动 kill，防止超时后孤儿进程
    cmd.kill_on_drop(true);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            error!("Failed to spawn process: {}", e);
            let _ = tx
                .send(Ok(TaskResponse {
                    task_id: req.task_id.clone(),
                    status: TaskStatus::Failed as i32,
                    log_chunk: format!("Failed to spawn process: {}", e),
                    final_result: String::new(),
                }))
                .await;
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
        loop {
            match stdout_reader.next_line().await {
                Ok(Some(line)) => {
                    let _ = tx_stdout
                        .send(Ok(TaskResponse {
                            task_id: task_id_stdout.clone(),
                            status: TaskStatus::Running as i32,
                            log_chunk: format!("OUT: {}\n", line),
                            final_result: String::new(),
                        }))
                        .await;
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Error reading stdout for task {}: {}", task_id_stdout, e);
                    break;
                }
            }
        }
    });

    let tx_stderr = tx.clone();
    let task_id_stderr = req.task_id.clone();
    let stderr_handle = tokio::spawn(async move {
        loop {
            match stderr_reader.next_line().await {
                Ok(Some(line)) => {
                    let _ = tx_stderr
                        .send(Ok(TaskResponse {
                            task_id: task_id_stderr.clone(),
                            status: TaskStatus::Running as i32,
                            log_chunk: format!("ERR: {}\n", line),
                            final_result: String::new(),
                        }))
                        .await;
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Error reading stderr for task {}: {}", task_id_stderr, e);
                    break;
                }
            }
        }
    });

    // 超时包裹整个执行流程（stdout/stderr 读取 + child.wait）
    let timeout_duration = std::time::Duration::from_secs(if req.timeout_seconds > 0 {
        req.timeout_seconds as u64
    } else {
        DEFAULT_TIMEOUT_SECS
    });

    let task_id_for_timeout = req.task_id.clone();
    let timeout_secs_for_log = req.timeout_seconds;

    match tokio::time::timeout(timeout_duration, async {
        let _ = tokio::join!(stdout_handle, stderr_handle);
        child.wait().await
    })
    .await
    {
        Ok(Ok(exit_status)) if exit_status.success() => {
            info!("Task {} finished successfully", req.task_id);
            let _ = tx
                .send(Ok(TaskResponse {
                    task_id: req.task_id.clone(),
                    status: TaskStatus::Completed as i32,
                    log_chunk: "Process finished successfully.\n".to_string(),
                    final_result: "Success".to_string(),
                }))
                .await;
        }
        Ok(Ok(exit_status)) => {
            error!("Task {} failed with status {}", req.task_id, exit_status);
            let _ = tx
                .send(Ok(TaskResponse {
                    task_id: req.task_id.clone(),
                    status: TaskStatus::Failed as i32,
                    log_chunk: format!("Process finished with error status: {}\n", exit_status),
                    final_result: String::new(),
                }))
                .await;
        }
        Ok(Err(e)) => {
            error!("Task {} process wait error: {}", req.task_id, e);
            let _ = tx
                .send(Ok(TaskResponse {
                    task_id: req.task_id.clone(),
                    status: TaskStatus::Failed as i32,
                    log_chunk: format!("Wait process error: {}\n", e),
                    final_result: String::new(),
                }))
                .await;
        }
        Err(_) => {
            // 超时：child 已被 kill_on_drop 自动终止
            error!(
                "Task {} timed out after {}s",
                task_id_for_timeout, timeout_secs_for_log
            );
            let _ = tx
                .send(Ok(TaskResponse {
                    task_id: task_id_for_timeout,
                    status: TaskStatus::Timeout as i32,
                    log_chunk: format!(
                        "Process timed out and was killed after {}s.\n",
                        timeout_secs_for_log
                    ),
                    final_result: String::new(),
                }))
                .await;
        }
    }
}
