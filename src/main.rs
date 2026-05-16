pub mod pipeline {
    tonic::include_proto!("gege");
}

pub mod executors;
pub mod server;
pub mod task_manager;
pub mod ssh_tunnel;
pub mod http_server;

use log::info;
use pipeline::agent_pipeline_server::AgentPipelineServer;
use server::AgentPipelineService;
use std::net::SocketAddr;
use tonic::transport::Server;
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    // Check if SSH tunneling is configured
    if let Some(ssh_config) = ssh_tunnel::SshConfig::from_env() {
        tokio::spawn(async move {
            ssh_tunnel::start_ssh_tunnel(ssh_config).await;
        });
    }

    let local_port = env::var("GEGE_LOCAL_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50051);
        
    let http_port = env::var("GEGE_HTTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8081);

    // Shared Task Manager for both gRPC and HTTP
    let task_manager = Arc::new(task_manager::TaskManager::new());

    // Spawn HTTP Server
    let http_task_manager = task_manager.clone();
    tokio::spawn(async move {
        http_server::start_http_server(http_task_manager, http_port).await;
    });

    let addr: SocketAddr = format!("127.0.0.1:{}", local_port).parse()?;
    
    // Inject shared task manager to gRPC service
    let pipeline_service = AgentPipelineService::with_manager(task_manager);

    info!("Gege Proxy Layer Server listening on {}", addr);

    Server::builder()
        .add_service(AgentPipelineServer::new(pipeline_service))
        .serve(addr)
        .await?;

    Ok(())
}
