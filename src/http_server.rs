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
    pub expected_bearer: String,
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
    pub metadata_json: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct SubmitTaskResponse {
    pub accepted: bool,
    pub message: String,
}

#[derive(Serialize)]
pub struct CancelTaskResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Serialize)]
pub struct TaskStatusJson {
    pub exists: bool,
    pub status: String,
    pub message: String,
}

/// 校验 Bearer Token（常量时间比较，防时序攻击）
fn verify_bearer_token(expected: &str, actual: &str) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    expected.as_bytes().ct_eq(actual.as_bytes()).into()
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

    if !verify_bearer_token(&state.expected_bearer, auth_header) {
        return Err((StatusCode::UNAUTHORIZED, "Invalid auth token".to_string()));
    }

    Ok(next.run(req).await)
}

async fn health_check() -> &'static str {
    "ok"
}

pub async fn start_http_server(
    task_manager: Arc<TaskManager>,
    port: u16,
    auth_token: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let state = AppState {
        task_manager,
        expected_bearer: format!("Bearer {}", auth_token),
    };

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/v1/tasks", post(submit_task))
        .route("/v1/tasks/{task_id}", get(get_task_status).delete(cancel_task))
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware))
        .with_state(state);

    let addr = format!("127.0.0.1:{}", port);
    info!("Gege HTTP REST API listening on {}", addr);

    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            info!("Failed to bind HTTP server on {}: {}", addr, e);
            return;
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap_or_else(|e| {
            info!("HTTP server error: {}", e);
        });
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
        metadata_json: payload.metadata_json.unwrap_or_default(),
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
) -> Result<Json<CancelTaskResponse>, (StatusCode, String)> {
    let success = state.task_manager.cancel_task(&task_id).await;

    Ok(Json(CancelTaskResponse {
        success,
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
    use axum::http::StatusCode;

    fn test_state() -> AppState {
        AppState {
            task_manager: Arc::new(TaskManager::new()),
            expected_bearer: "Bearer test-token".to_string(),
        }
    }

    #[test]
    fn test_verify_bearer_token_valid() {
        assert!(verify_bearer_token("Bearer test-token", "Bearer test-token"));
    }

    #[test]
    fn test_verify_bearer_token_invalid() {
        assert!(!verify_bearer_token("Bearer test-token", "Bearer wrong-token"));
    }

    #[test]
    fn test_verify_bearer_token_empty() {
        assert!(!verify_bearer_token("Bearer test-token", ""));
        assert!(!verify_bearer_token("", "Bearer test-token"));
    }

    #[test]
    fn test_verify_bearer_token_length_mismatch() {
        assert!(!verify_bearer_token("Bearer test-token", "Bearer x"));
    }

    #[tokio::test]
    async fn test_get_task_status_existing() {
        let state = test_state();
        let req = TaskRequest {
            task_id: "status-task".to_string(),
            agent_type: crate::pipeline::AgentType::Unknown as i32,
            prompt: "test".to_string(),
            workspace_dir: "".to_string(),
            env_vars: Default::default(),
            timeout_seconds: 3600,
            auth_token: "token".to_string(),
            callback_url: "".to_string(),
            callback_headers: Default::default(),
            callback_format: 0,
            metadata_json: String::new(),
        };
        let _ = state.task_manager.start_task(req, None).await;

        let result = get_task_status(State(state), Path("status-task".to_string())).await;
        assert!(result.is_ok());
        let json = result.unwrap().0;
        assert!(json.exists);
    }

    #[tokio::test]
    async fn test_get_task_status_not_found() {
        let state = test_state();
        let result = get_task_status(State(state), Path("no-such-task".to_string())).await;
        assert!(result.is_ok());
        let json = result.unwrap().0;
        assert!(!json.exists);
    }

    #[tokio::test]
    async fn test_cancel_task_existing() {
        let state = test_state();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        });
        state.task_manager.tasks.lock().await.insert(
            "cancel-task".to_string(),
            crate::task_manager::TaskEntry {
                handle: Some(handle),
                status: crate::pipeline::TaskStatus::Running as i32,
                message: "Running".to_string(),
                completed_at: None,
            },
        );

        let result = cancel_task(State(state), Path("cancel-task".to_string())).await;
        assert!(result.is_ok());
        let json = result.unwrap().0;
        assert!(json.success);
    }

    #[tokio::test]
    async fn test_cancel_task_not_found() {
        let state = test_state();
        let result = cancel_task(State(state), Path("no-such-task".to_string())).await;
        assert!(result.is_ok());
        let json = result.unwrap().0;
        assert!(!json.success);
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
            metadata_json: None,
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
            metadata_json: None,
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
            metadata_json: None,
        };

        let result = submit_task(State(state), Json(payload)).await;
        assert!(result.is_ok());
        assert!(result.unwrap().accepted);
    }

    #[tokio::test]
    async fn test_health_check() {
        let result = health_check().await;
        assert_eq!(result, "ok");
    }
}
