use log::{error, info, warn};
use std::env;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::sleep;

pub struct SshConfig {
    pub remote_host: String,
    pub remote_port: u16,
    pub remote_user: String,
    pub bind_port: u16,
    pub local_port: u16,
    pub key_path: Option<String>,
}

impl SshConfig {
    pub fn from_env() -> Option<Self> {
        let remote_host = env::var("GEGE_SSH_REMOTE_HOST").ok()?;
        
        let remote_port = env::var("GEGE_SSH_REMOTE_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(22);
            
        let remote_user = env::var("GEGE_SSH_USER").unwrap_or_else(|_| "root".to_string());
        
        let bind_port = env::var("GEGE_SSH_BIND_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50051);
            
        let local_port = env::var("GEGE_LOCAL_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50051);
            
        let key_path = env::var("GEGE_SSH_KEY_PATH").ok();

        Some(Self {
            remote_host,
            remote_port,
            remote_user,
            bind_port,
            local_port,
            key_path,
        })
    }
}

pub async fn start_ssh_tunnel(config: SshConfig) {
    info!(
        "Starting SSH tunnel mapping Remote {}:{} to Local 127.0.0.1:{}",
        config.remote_host, config.bind_port, config.local_port
    );

    loop {
        let mut cmd = Command::new("ssh");

        // -N: 不执行远程命令
        // -R: 远端端口转发 (BindPort:127.0.0.1:LocalPort)
        let forward_str = format!("{}:127.0.0.1:{}", config.bind_port, config.local_port);
        cmd.arg("-N").arg("-R").arg(&forward_str);

        if let Some(ref key) = config.key_path {
            cmd.arg("-i").arg(key);
        }

        cmd.arg("-p")
            .arg(config.remote_port.to_string())
            .arg("-o")
            .arg("StrictHostKeyChecking=accept-new") // 避免初次连接阻塞
            .arg("-o")
            .arg("ServerAliveInterval=60") // 保持心跳防断
            .arg("-o")
            .arg("ExitOnForwardFailure=yes") // 如果远端端口被占用，直接退出重试
            .arg(format!("{}@{}", config.remote_user, config.remote_host));

        info!("Executing: {:?}", cmd);

        match cmd.spawn() {
            Ok(mut child) => {
                match child.wait().await {
                    Ok(status) => {
                        warn!("SSH tunnel exited with status: {}. Restarting in 5s...", status);
                    }
                    Err(e) => {
                        error!("Failed to wait on SSH tunnel: {}. Restarting in 5s...", e);
                    }
                }
            }
            Err(e) => {
                error!("Failed to spawn SSH process: {}. Is 'ssh' installed? Restarting in 5s...", e);
                sleep(Duration::from_secs(5)).await;
            }
        }

        sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_ssh_config_from_env_missing_host() {
        env::remove_var("GEGE_SSH_REMOTE_HOST");
        let config = SshConfig::from_env();
        assert!(config.is_none(), "Should be none when host is missing");
    }

    #[test]
    fn test_ssh_config_from_env_with_host() {
        env::set_var("GEGE_SSH_REMOTE_HOST", "test.example.com");
        env::set_var("GEGE_SSH_USER", "testuser");
        env::set_var("GEGE_LOCAL_PORT", "12345");
        
        let config = SshConfig::from_env().expect("Config should be created");
        
        assert_eq!(config.remote_host, "test.example.com");
        assert_eq!(config.remote_user, "testuser");
        assert_eq!(config.local_port, 12345);
        assert_eq!(config.bind_port, 50051); // default
        assert_eq!(config.remote_port, 22); // default
        
        // Clean up
        env::remove_var("GEGE_SSH_REMOTE_HOST");
        env::remove_var("GEGE_SSH_USER");
        env::remove_var("GEGE_LOCAL_PORT");
    }
}
