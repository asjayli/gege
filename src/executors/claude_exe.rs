use crate::executors::{execute_command, AgentExecutor};
use crate::pipeline::{TaskRequest, TaskResponse};
use async_trait::async_trait;
use tokio::process::Command;
use tokio::sync::mpsc;
use tonic::Status;

pub struct ClaudeExecutor;

#[async_trait]
impl AgentExecutor for ClaudeExecutor {
    async fn execute(&self, req: &TaskRequest, tx: mpsc::Sender<Result<TaskResponse, Status>>) {
        let mut cmd = Command::new("claude");
        cmd.arg("-p").arg(&req.prompt);
        execute_command(cmd, req, tx).await;
    }
}
