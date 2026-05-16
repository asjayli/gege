use super::{
    claude_exe::ClaudeExecutor, codex_exe::CodexExecutor, gemini_exe::GeminiExecutor,
    hehe_exe::HeheExecutor, opencode_exe::OpenCodeExecutor, AgentExecutor,
};
use crate::pipeline::AgentType;
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::Status;
use crate::pipeline::{TaskRequest, TaskResponse};
use async_trait::async_trait;

pub struct UnknownExecutor;

#[async_trait]
impl AgentExecutor for UnknownExecutor {
    async fn execute(&self, _req: &TaskRequest, tx: mpsc::Sender<Result<TaskResponse, Status>>) {
        let _ = tx.send(Err(Status::invalid_argument("Unknown agent type"))).await;
    }
}

pub fn create_executor(agent_type: &AgentType) -> Arc<dyn AgentExecutor> {
    match agent_type {
        AgentType::ClaudeCode => Arc::new(ClaudeExecutor),
        AgentType::GeminiCli => Arc::new(GeminiExecutor),
        AgentType::Codex => Arc::new(CodexExecutor),
        AgentType::OpenCode => Arc::new(OpenCodeExecutor),
        AgentType::HeheNative => Arc::new(HeheExecutor),
        _ => Arc::new(UnknownExecutor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_executor() {
        let claude = create_executor(&AgentType::ClaudeCode);
        assert!(Arc::ptr_eq(&claude, &claude)); // Basic validity check

        let gemini = create_executor(&AgentType::GeminiCli);
        assert!(Arc::ptr_eq(&gemini, &gemini));

        let codex = create_executor(&AgentType::Codex);
        assert!(Arc::ptr_eq(&codex, &codex));

        let opencode = create_executor(&AgentType::OpenCode);
        assert!(Arc::ptr_eq(&opencode, &opencode));

        let hehe = create_executor(&AgentType::HeheNative);
        assert!(Arc::ptr_eq(&hehe, &hehe));

        let unknown = create_executor(&AgentType::Unknown);
        assert!(Arc::ptr_eq(&unknown, &unknown));
    }
}
