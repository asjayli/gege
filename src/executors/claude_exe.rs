use crate::executors::{execute_command, prompt_wrapper, AgentExecutor};
use crate::pipeline::{TaskRequest, TaskResponse};
use async_trait::async_trait;
use tokio::process::Command;
use tokio::sync::mpsc;
use tonic::Status;

pub struct ClaudeExecutor;

#[async_trait]
impl AgentExecutor for ClaudeExecutor {
    async fn execute(&self, req: &TaskRequest, tx: mpsc::Sender<Result<TaskResponse, Status>>) {
        let wrapped_prompt = prompt_wrapper::wrap_prompt(&req.prompt);

        let mut cmd = Command::new("claude");
        cmd.arg("-p")
            .arg(&wrapped_prompt)
            .arg("--output-format")
            .arg("stream-json")
            .arg("--dangerously-skip-permissions");
        execute_command(
            cmd,
            req,
            tx,
            Some(prompt_wrapper::extract_result),
            Some(prompt_wrapper::parse_claude_line),
        )
        .await;
    }
}
