use crate::pipeline::{AgentType, CallbackFormat, TaskRequest};
use crate::task_manager::{task_status_to_string, TaskManager};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    middleware::{self, Next},
    extract::Request,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use log::info;
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub struct AppState {
    pub task_manager: Arc<TaskManager>,
    pub auth_token: String,
}

#[derive(Deserialize)]
pub struct SubmitTaskPayload {
    pub task_id: String,
    pub agent_type: String,
    pub prompt: String,
    pub workspace_dir: Option<String>,
    pub env_vars: Option<std::collections::HashMap<String, String>>,
    pub timeout_seconds: Option<i32>,
    pub callback_url: String,
    pub callback_headers: Option<std::collections::HashMap<String, String>>,
    pub callback_format: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct SubmitTaskResponse {
    pub accepted: bool,
    pub message: String,
}

#[derive(Serialize)]
pub struct TaskStatusJson {
    pub exists: bool,
    pub status: String,
    pub message: String,
}

/// 鉴权中间件：校验 Authorization: Bearer <token>
async fn auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<axum::response::Response, (StatusCode, String)> {
    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let expected = format!("Bearer {}", state.auth_token);
    let equal: bool = expected.as_bytes().ct_eq(auth_header.as_bytes()).into();
    if !equal {
        return Err((StatusCode::UNAUTHORIZED, "Invalid auth token".to_string()));
    }

    Ok(next.run(req).await)
}

pub async fn start_http_server(task_manager: Arc<TaskManager>, port: u16, auth_token: String) {
    let state = AppState {
        task_manager,
        auth_token,
    };

    let app = Router::new()
        .route("/v1/tasks", post(submit_task))
        .route("/v1/tasks/{task_id}", get(get_task_status).delete(cancel_task))
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware))
        .with_state(state);

    let addr = format!("127.0.0.1:{}", port);
    info!("Gege HTTP REST API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn submit_task(
    State(state): State<AppState>,
    Json(payload): Json<SubmitTaskPayload>,
) -> Result<Json<SubmitTaskResponse>, (StatusCode, String)> {
    if payload.callback_url.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "callback_url is required".to_string(),
        ));
    }

    let agent_type = match payload.agent_type.as_str() {
        "CLAUDE_CODE" => AgentType::ClaudeCode,
        "GEMINI_CLI" => AgentType::GeminiCli,
        "CODEX" => AgentType::Codex,
        "OPEN_CODE" => AgentType::OpenCode,
        "HEHE_NATIVE" => AgentType::HeheNative,
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Invalid agent_type: {}", payload.agent_type),
            ));
        }
    };

    let callback_format = match payload.callback_format.as_deref() {
        Some("FEISHU_BOT") => CallbackFormat::FeishuBot,
        Some("WECOM_BOT") => CallbackFormat::WecomBot,
        _ => CallbackFormat::Default,
    };

    let req = TaskRequest {
        task_id: payload.task_id.clone(),
        agent_type: agent_type as i32,
        prompt: payload.prompt,
        workspace_dir: payload.workspace_dir.unwrap_or_default(),
        env_vars: payload.env_vars.unwrap_or_default(),
        timeout_seconds: payload.timeout_seconds.unwrap_or(3600),
        auth_token: String::new(), // 鉴权已由中间件处理
        callback_url: payload.callback_url,
        callback_headers: payload.callback_headers.unwrap_or_default(),
        callback_format: callback_format as i32,
    };

    if let Err(e) = state.task_manager.start_task(req, None).await {
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

async fn get_task_status(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskStatusJson>, (StatusCode, String)> {
    let status = state.task_manager.get_task_status(&task_id).await;

    Ok(Json(TaskStatusJson {
        exists: status.exists,
        status: task_status_to_string(status.status).to_string(),
        message: status.message,
    }))
}

async fn cancel_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<SubmitTaskResponse>, (StatusCode, String)> {
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

    fn test_state() -> AppState {
        AppState {
            task_manager: Arc::new(TaskManager::new()),
            auth_token: "test-token".to_string(),
        }
    }

    #[tokio::test]
    async fn test_submit_task_missing_callback() {
        let state = test_state();
        let payload = SubmitTaskPayload {
            task_id: "test1".to_string(),
            agent_type: "CLAUDE_CODE".to_string(),
            prompt: "hello".to_string(),
            workspace_dir: None,
            env_vars: None,
            timeout_seconds: None,
            callback_url: "".to_string(),
            callback_headers: None,
            callback_format: None,
        };

        let result = submit_task(State(state), Json(payload)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_submit_task_invalid_agent_type() {
        let state = test_state();
        let payload = SubmitTaskPayload {
            task_id: "test1".to_string(),
            agent_type: "INVALID_AGENT".to_string(),
            prompt: "hello".to_string(),
            workspace_dir: None,
            env_vars: None,
            timeout_seconds: None,
            callback_url: "http://localhost/cb".to_string(),
            callback_headers: None,
            callback_format: None,
        };

        let result = submit_task(State(state), Json(payload)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_submit_task_success() {
        let state = test_state();
        let payload = SubmitTaskPayload {
            task_id: "test1".to_string(),
            agent_type: "CLAUDE_CODE".to_string(),
            prompt: "hello".to_string(),
            workspace_dir: None,
            env_vars: None,
            timeout_seconds: None,
            callback_url: "http://localhost/cb".to_string(),
            callback_headers: None,
            callback_format: None,
        };

        let result = submit_task(State(state), Json(payload)).await;
        assert!(result.is_ok());
        assert!(result.unwrap().accepted);
    }
}
