use crate::pipeline::{TaskRequest, TaskResponse};
use crate::executors::{execute_command, AgentExecutor};
use async_trait::async_trait;
use tokio::process::Command;
use tokio::sync::mpsc;
use tonic::Status;

pub struct HeheExecutor;

#[async_trait]
impl AgentExecutor for HeheExecutor {
    async fn execute(&self, req: &TaskRequest, tx: mpsc::Sender<Result<TaskResponse, Status>>) {
        let mut cmd = Command::new("hehe");
        cmd.arg("run").arg("--instruction").arg(&req.prompt);
        execute_command(cmd, req, tx).await;
    }
}
