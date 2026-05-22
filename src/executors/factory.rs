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
    use crate::pipeline::AgentType;

    #[tokio::test]
    async fn test_unknown_executor_returns_error() {
        let executor = create_executor(&AgentType::Unknown);
        let (tx, mut rx) = mpsc::channel(1);

        let req = TaskRequest {
            task_id: "test".to_string(),
            agent_type: AgentType::Unknown as i32,
            prompt: String::new(),
            workspace_dir: String::new(),
            env_vars: Default::default(),
            timeout_seconds: 0,
            auth_token: String::new(),
            callback_url: String::new(),
            callback_headers: Default::default(),
            callback_format: 0,
            metadata_json: String::new(),
        };

        executor.execute(&req, tx).await;
        let result = rx.recv().await.unwrap();
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("Unknown agent type"));
    }

    #[test]
    fn test_create_executor_all_types() {
        // 验证各类型都能成功创建 executor
        let _ = create_executor(&AgentType::ClaudeCode);
        let _ = create_executor(&AgentType::GeminiCli);
        let _ = create_executor(&AgentType::Codex);
        let _ = create_executor(&AgentType::OpenCode);
        let _ = create_executor(&AgentType::HeheNative);
        let _ = create_executor(&AgentType::Unknown);
    }
}
