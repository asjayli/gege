pub mod pipeline {
    tonic::include_proto!("gege");
}

pub mod config;
pub mod executors;
pub mod repair;
pub mod server;
pub mod task_manager;
pub mod ssh_tunnel;
pub mod http_server;

use log::info;
use pipeline::agent_pipeline_server::AgentPipelineServer;
use server::AgentPipelineService;
use std::net::SocketAddr;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let cfg = config::GegeConfig::from_env();

    // SSH 隧道（可选）
    if let Some(ssh_config) = ssh_tunnel::SshConfig::from_env() {
        tokio::spawn(async move {
            ssh_tunnel::start_ssh_tunnel(ssh_config).await;
        });
    }

    // 共享 TaskManager
    let task_manager = std::sync::Arc::new(task_manager::TaskManager::new());

    // HTTP Server 停机信号
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // HTTP Server
    let http_tm = task_manager.clone();
    let http_auth = cfg.auth_token.clone();
    let http_port = cfg.http_port;
    tokio::spawn(async move {
        http_server::start_http_server(http_tm, http_port, http_auth, http_shutdown_rx).await;
    });

    // gRPC Server + 优雅停机
    let addr: SocketAddr = format!("127.0.0.1:{}", cfg.local_port).parse()?;
    let pipeline_service =
        AgentPipelineService::with_manager(task_manager, cfg.auth_token.clone());

    info!("Gege Proxy Layer Server listening on {}", addr);

    let (grpc_shutdown_tx, grpc_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal, stopping gracefully...");
        let _ = grpc_shutdown_tx.send(());
        let _ = http_shutdown_tx.send(());
    });

    Server::builder()
        .add_service(AgentPipelineServer::new(pipeline_service))
        .serve_with_shutdown(addr, async {
            grpc_shutdown_rx.await.ok();
        })
        .await?;

    info!("Gege server shut down.");
    Ok(())
}
