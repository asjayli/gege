use crate::pipeline::{TaskRequest, AgentType};
use crate::task_manager::TaskManager;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use log::{info, error};

#[derive(Clone)]
pub struct AppState {
    pub task_manager: Arc<TaskManager>,
}

#[derive(Deserialize)]
pub struct SubmitTaskPayload {
    pub task_id: String,
    pub agent_type: String, // "CLAUDE_CODE", "GEMINI_CLI", "CODEX", "OPEN_CODE", "HEHE_NATIVE"
    pub prompt: String,
    pub workspace_dir: Option<String>,
    pub env_vars: Option<std::collections::HashMap<String, String>>,
    pub timeout_seconds: Option<i32>,
    pub auth_token: String,
    pub callback_url: String,
    pub callback_headers: Option<std::collections::HashMap<String, String>>,
    pub callback_format: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct SubmitTaskResponse {
    pub accepted: bool,
    pub message: String,
}

#[derive(Deserialize)]
pub struct StatusPayload {
    pub auth_token: String,
}

#[derive(Serialize)]
pub struct TaskStatusJson {
    pub exists: bool,
    pub status: String,
    pub message: String,
}

// 启动HTTP服务
pub async fn start_http_server(task_manager: Arc<TaskManager>, port: u16) {
    let state = AppState { task_manager };

    let app = Router::new()
        .route("/v1/tasks/submit", post(submit_task))
        .route("/v1/tasks/:task_id/status", post(get_task_status))
        .route("/v1/tasks/:task_id/cancel", post(cancel_task))
        .with_state(state);

    let addr = format!("127.0.0.1:{}", port);
    info!("Gege HTTP REST API listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// 处理任务提交
async fn submit_task(
    State(state): State<AppState>,
    Json(payload): Json<SubmitTaskPayload>,
) -> Result<Json<SubmitTaskResponse>, (StatusCode, String)> {
    // Auth Check
    if payload.auth_token != "hehe-super-secret-token" {
        return Err((StatusCode::UNAUTHORIZED, "Invalid auth token".to_string()));
    }

    if payload.callback_url.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "callback_url is required".to_string()));
    }

    let agent_type = match payload.agent_type.as_str() {
        "CLAUDE_CODE" => AgentType::ClaudeCode,
        "GEMINI_CLI" => AgentType::GeminiCli,
        "CODEX" => AgentType::Codex,
        "OPEN_CODE" => AgentType::OpenCode,
        "HEHE_NATIVE" => AgentType::HeheNative,
        _ => AgentType::Unknown,
    };

    let callback_format = match payload.callback_format.as_deref() {
        Some("FEISHU_BOT") => crate::pipeline::CallbackFormat::FeishuBot,
        Some("WECOM_BOT") => crate::pipeline::CallbackFormat::WecomBot,
        _ => crate::pipeline::CallbackFormat::Default,
    };

    let req = TaskRequest {
        task_id: payload.task_id.clone(),
        agent_type: agent_type as i32,
        prompt: payload.prompt,
        workspace_dir: payload.workspace_dir.unwrap_or_default(),
        env_vars: payload.env_vars.unwrap_or_default(),
        timeout_seconds: payload.timeout_seconds.unwrap_or(3600),
        auth_token: payload.auth_token,
        callback_url: payload.callback_url,
        callback_headers: payload.callback_headers.unwrap_or_default(),
        callback_format: callback_format as i32,
    };

    if let Err(e) = state.task_manager.start_task(req, None).await {
        error!("Failed to submit HTTP task: {}", e);
        return Ok(Json(SubmitTaskResponse {
            accepted: false,
            message: format!("Failed to start task: {}", e),
        }));
    }

    Ok(Json(SubmitTaskResponse {
        accepted: true,
        message: "Task has been accepted and is running in background".to_string(),
    }))
}

// 获取任务状态
async fn get_task_status(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(payload): Json<StatusPayload>,
) -> Result<Json<TaskStatusJson>, (StatusCode, String)> {
    if payload.auth_token != "hehe-super-secret-token" {
        return Err((StatusCode::UNAUTHORIZED, "Invalid auth token".to_string()));
    }

    let status = state.task_manager.get_task_status(&task_id).await;
    
    // Convert protobuf enum status to string
    let status_str = match status.status() {
        crate::pipeline::TaskStatus::Running => "RUNNING",
        crate::pipeline::TaskStatus::Completed => "COMPLETED",
        crate::pipeline::TaskStatus::Failed => "FAILED",
        crate::pipeline::TaskStatus::Timeout => "TIMEOUT",
        crate::pipeline::TaskStatus::Cancelled => "CANCELLED",
    };

    Ok(Json(TaskStatusJson {
        exists: status.exists,
        status: status_str.to_string(),
        message: status.message,
    }))
}

// 取消任务
async fn cancel_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(payload): Json<StatusPayload>,
) -> Result<Json<SubmitTaskResponse>, (StatusCode, String)> {
    if payload.auth_token != "hehe-super-secret-token" {
        return Err((StatusCode::UNAUTHORIZED, "Invalid auth token".to_string()));
    }

    let success = state.task_manager.cancel_task(&task_id).await;
    
    Ok(Json(SubmitTaskResponse {
        accepted: success,
        message: if success {
            "Task cancelled successfully".to_string()
        } else {
            "Task not found or already completed".to_string()
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_manager::TaskManager;

    #[tokio::test]
    async fn test_submit_task_unauthorized() {
        let state = AppState {
            task_manager: Arc::new(TaskManager::new()),
        };
        
        let payload = SubmitTaskPayload {
            task_id: "test1".to_string(),
            agent_type: "UNKNOWN".to_string(),
            prompt: "hello".to_string(),
            workspace_dir: None,
            env_vars: None,
            timeout_seconds: None,
            auth_token: "wrong-token".to_string(),
            callback_url: "http://localhost/cb".to_string(),
            callback_headers: None,
            callback_format: None,
        };

        let result = submit_task(State(state), Json(payload)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_submit_task_missing_callback() {
        let state = AppState {
            task_manager: Arc::new(TaskManager::new()),
        };
        
        let payload = SubmitTaskPayload {
            task_id: "test1".to_string(),
            agent_type: "UNKNOWN".to_string(),
            prompt: "hello".to_string(),
            workspace_dir: None,
            env_vars: None,
            timeout_seconds: None,
            auth_token: "hehe-super-secret-token".to_string(),
            callback_url: "".to_string(),
            callback_headers: None,
            callback_format: None,
        };

        let result = submit_task(State(state), Json(payload)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_submit_task_success() {
        let state = AppState {
            task_manager: Arc::new(TaskManager::new()),
        };
        
        let payload = SubmitTaskPayload {
            task_id: "test1".to_string(),
            agent_type: "UNKNOWN".to_string(),
            prompt: "hello".to_string(),
            workspace_dir: None,
            env_vars: None,
            timeout_seconds: None,
            auth_token: "hehe-super-secret-token".to_string(),
            callback_url: "http://localhost/cb".to_string(),
            callback_headers: None,
            callback_format: None,
        };

        let result = submit_task(State(state), Json(payload)).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.accepted);
    }
}