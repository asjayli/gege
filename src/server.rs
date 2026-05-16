use crate::pipeline::{
    agent_pipeline_server::AgentPipeline, CancelRequest, CancelResponse, SubmitResponse, TaskRequest,
    TaskResponse,
};
use crate::task_manager::TaskManager;
use log::{error, info};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct AgentPipelineService {
    task_manager: Arc<TaskManager>,
    auth_token: String,
}

impl AgentPipelineService {
    pub fn with_manager(manager: Arc<TaskManager>, auth_token: String) -> Self {
        Self {
            task_manager: manager,
            auth_token,
        }
    }

    #[allow(clippy::result_large_err)]
    fn check_auth(&self, token: &str) -> Result<(), Status> {
        if token != self.auth_token {
            return Err(Status::unauthenticated("Invalid auth token"));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl AgentPipeline for AgentPipelineService {
    type ExecuteTaskStreamStream = ReceiverStream<Result<TaskResponse, Status>>;

    async fn execute_task_stream(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStreamStream>, Status> {
        let req = request.into_inner();
        self.check_auth(&req.auth_token)?;

        let task_id = req.task_id.clone();
        info!("Received streaming task request: {}", task_id);

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        if let Err(e) = self.task_manager.start_task(req, Some(tx)).await {
            error!("Failed to submit streaming task {}: {}", task_id, e);
            return Err(Status::internal("Failed to start task"));
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn submit_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<SubmitResponse>, Status> {
        let req = request.into_inner();
        self.check_auth(&req.auth_token)?;

        let task_id = req.task_id.clone();
        info!("Received submit task request: {}", task_id);

        if req.callback_url.is_empty() {
            return Err(Status::invalid_argument("callback_url is required for SubmitTask"));
        }

        if let Err(e) = self.task_manager.start_task(req, None).await {
            error!("Failed to submit task {}: {}", task_id, e);
            return Ok(Response::new(SubmitResponse {
                accepted: false,
                message: format!("Failed to start task: {}", e),
            }));
        }

        Ok(Response::new(SubmitResponse {
            accepted: true,
            message: "Task has been accepted and is running in background".to_string(),
        }))
    }

    async fn cancel_task(
        &self,
        request: Request<CancelRequest>,
    ) -> Result<Response<CancelResponse>, Status> {
        let req = request.into_inner();
        self.check_auth(&req.auth_token)?;

        info!("Received cancel request for task: {}", req.task_id);

        let success = self.task_manager.cancel_task(&req.task_id).await;
        Ok(Response::new(CancelResponse {
            success,
            message: if success {
                "Task cancelled successfully".to_string()
            } else {
                "Task not found or already completed".to_string()
            },
        }))
    }

    async fn get_task_status(
        &self,
        request: Request<crate::pipeline::GetStatusRequest>,
    ) -> Result<Response<crate::pipeline::TaskStatusResponse>, Status> {
        let req = request.into_inner();
        self.check_auth(&req.auth_token)?;

        let status = self.task_manager.get_task_status(&req.task_id).await;
        Ok(Response::new(status))
    }
}
