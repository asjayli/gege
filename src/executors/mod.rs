use crate::config::DANGEROUS_ENV_VARS;
use crate::executors::prompt_wrapper::ParsedOutput;
use crate::pipeline::{TaskRequest, TaskResponse, TaskStatus};
use crate::repair;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tonic::Status;
use log::{error, info, warn};

pub mod claude_exe;
pub mod codex_exe;
pub mod factory;
pub mod gemini_exe;
pub mod hehe_exe;
pub mod opencode_exe;
pub mod prompt_wrapper;

#[async_trait]
pub trait AgentExecutor: Send + Sync {
    async fn execute(&self, req: &TaskRequest, tx: mpsc::Sender<Result<TaskResponse, Status>>);
}

/// 单行 stdout 解析结果
pub struct ParsedLine {
    pub agent_text: String,
    pub agent_raw: String,
}

/// 校验 workspace_dir 是否安全（防止路径遍历）
fn validate_workspace(workspace: &str) -> Result<PathBuf, String> {
    if workspace.is_empty() {
        return Ok(PathBuf::new());
    }

    let path = Path::new(workspace);

    if !path.is_absolute() {
        return Err(format!(
            "Invalid workspace_dir: '{}' must be an absolute path",
            workspace
        ));
    }

    let canonical = path
        .canonicalize()
        .map_err(|e| format!("Invalid workspace_dir: '{}' - {}", workspace, e))?;

    if !canonical.is_dir() {
        return Err(format!(
            "Invalid workspace_dir: '{}' is not a directory",
            workspace
        ));
    }

    Ok(canonical)
}

/// 提取一个通用的执行命令行任务的辅助函数
///
/// `result_extractor` — 从累积输出中提取最终结构化结果
/// `stdout_parser` — 可选的逐行解析器（Claude 用于从 stream-json 提取文本）
pub async fn execute_command(
    mut cmd: Command,
    req: &TaskRequest,
    tx: mpsc::Sender<Result<TaskResponse, Status>>,
    result_extractor: Option<fn(&str) -> ParsedOutput>,
    stdout_parser: Option<fn(&str) -> ParsedLine>,
) {
    // 校验并设置工作目录
    if !req.workspace_dir.is_empty() {
        match validate_workspace(&req.workspace_dir) {
            Ok(canonical) => {
                cmd.current_dir(canonical);
            }
            Err(msg) => {
                error!("{}", msg);
                let _ = tx
                    .send(Ok(TaskResponse {
                        task_id: req.task_id.clone(),
                        status: TaskStatus::Failed as i32,
                        log_chunk: msg,
                        final_result: String::new(),
                        agent_text: String::new(),
                        agent_raw: String::new(),
                        session_id: String::new(),
                        parsed: false,
                    }))
                    .await;
                return;
            }
        }
    }

    // 过滤危险环境变量
    for (k, v) in &req.env_vars {
        if DANGEROUS_ENV_VARS.contains(&k.as_str()) {
            warn!(
                "Blocked dangerous env var injection: {} for task {}",
                k, req.task_id
            );
            continue;
        }
        cmd.env(k, v);
    }

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
                    agent_text: String::new(),
                    agent_raw: String::new(),
                    session_id: String::new(),
                    parsed: false,
                }))
                .await;
            return;
        }
    };

    let stdout = child.stdout.take().expect("Failed to open stdout");
    let stderr = child.stderr.take().expect("Failed to open stderr");

    let mut stdout_reader = BufReader::new(stdout).lines();
    let mut stderr_reader = BufReader::new(stderr).lines();

    // 累积 agent stdout 文本
    let accumulated: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));

    let tx_stdout = tx.clone();
    let task_id_stdout = req.task_id.clone();
    let acc_stdout = accumulated.clone();
    let stdout_handle = tokio::spawn(async move {
        loop {
            match stdout_reader.next_line().await {
                Ok(Some(line)) => {
                    {
                        let mut acc = acc_stdout.lock().await;
                        if !acc.is_empty() {
                            acc.push('\n');
                        }
                        acc.push_str(&line);
                    }
                    // 逐行解析
                    let parsed = match stdout_parser {
                        Some(parser) => parser(&line),
                        None => ParsedLine {
                            agent_text: line.clone(),
                            agent_raw: line,
                        },
                    };
                    let _ = tx_stdout
                        .send(Ok(TaskResponse {
                            task_id: task_id_stdout.clone(),
                            status: TaskStatus::Running as i32,
                            log_chunk: format!("OUT: {}\n", parsed.agent_raw),
                            final_result: String::new(),
                            agent_text: parsed.agent_text,
                            agent_raw: parsed.agent_raw,
                            session_id: String::new(),
                            parsed: false,
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
                            agent_text: line.clone(),
                            agent_raw: line,
                            session_id: String::new(),
                            parsed: false,
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

    let timeout_duration = if req.timeout_seconds > 0 {
        Some(std::time::Duration::from_secs(req.timeout_seconds as u64))
    } else {
        None
    };

    let task_id_for_timeout = req.task_id.clone();
    let timeout_secs_for_log = req.timeout_seconds;

    let execution = async {
        let _ = tokio::join!(stdout_handle, stderr_handle);
        child.wait().await
    };

    let result = match timeout_duration {
        Some(dur) => tokio::time::timeout(dur, execution).await,
        None => Ok(execution.await),
    };

    let acc_text = accumulated.lock().await.clone();

    match result {
        Ok(Ok(exit_status)) if exit_status.success() => {
            let mut parsed_output = match result_extractor {
                Some(extract_fn) => extract_fn(&acc_text),
                None => ParsedOutput {
                    text: acc_text.trim().to_string(),
                    parsed: false,
                    ..Default::default()
                },
            };

            // 通用输出清洗：修复 agent 常见的输出问题
            let cleaned = repair::clean_agent_output(&parsed_output.text);
            if cleaned.is_json && cleaned.json_value.is_some() {
                // 成功修复为 JSON，使用修复后的文本
                parsed_output.text = cleaned.text;
            }

            info!("Task {} finished successfully", req.task_id);
            let _ = tx
                .send(Ok(TaskResponse {
                    task_id: req.task_id.clone(),
                    status: TaskStatus::Completed as i32,
                    log_chunk: "Process finished successfully.\n".to_string(),
                    final_result: parsed_output.text.clone(),
                    agent_text: parsed_output.text,
                    agent_raw: acc_text,
                    session_id: parsed_output.session_id,
                    parsed: parsed_output.parsed,
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
                    agent_text: String::new(),
                    agent_raw: acc_text,
                    session_id: String::new(),
                    parsed: false,
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
                    agent_text: String::new(),
                    agent_raw: acc_text,
                    session_id: String::new(),
                    parsed: false,
                }))
                .await;
        }
        Err(_) => {
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
                    agent_text: String::new(),
                    agent_raw: acc_text,
                    session_id: String::new(),
                    parsed: false,
                }))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_workspace_empty() {
        assert!(validate_workspace("").unwrap().as_os_str().is_empty());
    }

    #[test]
    fn test_validate_workspace_relative_path_rejected() {
        let result = validate_workspace("../some/path");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("absolute path"));
    }

    #[test]
    fn test_validate_workspace_nonexistent_rejected() {
        let result = validate_workspace("/nonexistent/path/that/does/not/exist");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_workspace_valid() {
        let result = validate_workspace("/tmp");
        assert!(result.is_ok());
    }
}
