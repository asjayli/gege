use log::{error, info, warn};
use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::process::Command;
use tokio::time::sleep;

/// 最大退避时间（秒）
const MAX_BACKOFF_SECS: u64 = 60;
/// 基础退避时间（秒）
const BASE_BACKOFF_SECS: u64 = 2;

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

    static RETRY_COUNT: AtomicU32 = AtomicU32::new(0);

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
            .arg(format!(
                "{}@{}",
                config.remote_user, config.remote_host
            ));

        info!("SSH tunnel connecting to {}@{}", config.remote_user, config.remote_host);

        match cmd.spawn() {
            Ok(mut child) => {
                match child.wait().await {
                    Ok(status) => {
                        warn!("SSH tunnel exited with status: {}. Restarting...", status);
                    }
                    Err(e) => {
                        error!("Failed to wait on SSH tunnel: {}. Restarting...", e);
                    }
                }
                // 连接过一次远端后重置退避
                RETRY_COUNT.store(0, Ordering::Relaxed);
            }
            Err(e) => {
                error!("Failed to spawn SSH process: {}. Is 'ssh' installed?", e);
            }
        }

        // 指数退避：2s, 4s, 8s, 16s, 32s, 60s, 60s, ...
        let retry = RETRY_COUNT.fetch_add(1, Ordering::Relaxed);
        let delay = (BASE_BACKOFF_SECS * 2u64.pow(retry)).min(MAX_BACKOFF_SECS);
        info!("SSH tunnel retrying in {}s...", delay);
        sleep(Duration::from_secs(delay)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;

    // 全局锁保证 SSH 测试串行执行，避免 env var 并发冲突
    static SSH_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn clear_ssh_env() {
        env::remove_var("GEGE_SSH_REMOTE_HOST");
        env::remove_var("GEGE_SSH_USER");
        env::remove_var("GEGE_LOCAL_PORT");
        env::remove_var("GEGE_SSH_REMOTE_PORT");
        env::remove_var("GEGE_SSH_BIND_PORT");
        env::remove_var("GEGE_SSH_KEY_PATH");
    }

    #[test]
    fn test_ssh_config_from_env_missing_host() {
        let _lock = SSH_TEST_LOCK.lock().unwrap();
        clear_ssh_env();
        let config = SshConfig::from_env();
        assert!(config.is_none(), "Should be none when host is missing");
    }

    #[test]
    fn test_ssh_config_from_env_defaults() {
        let _lock = SSH_TEST_LOCK.lock().unwrap();
        clear_ssh_env();
        env::set_var("GEGE_SSH_REMOTE_HOST", "test.example.com");

        let config = SshConfig::from_env().expect("Config should be created");

        assert_eq!(config.remote_host, "test.example.com");
        assert_eq!(config.remote_user, "root");
        assert_eq!(config.bind_port, 50051);
        assert_eq!(config.remote_port, 22);
        assert!(config.key_path.is_none());

        clear_ssh_env();
    }

    #[test]
    fn test_ssh_config_from_env_custom_values() {
        let _lock = SSH_TEST_LOCK.lock().unwrap();
        clear_ssh_env();
        env::set_var("GEGE_SSH_REMOTE_HOST", "my-server.com");
        env::set_var("GEGE_SSH_USER", "ubuntu");
        env::set_var("GEGE_LOCAL_PORT", "12345");
        env::set_var("GEGE_SSH_REMOTE_PORT", "2222");
        env::set_var("GEGE_SSH_BIND_PORT", "6000");
        env::set_var("GEGE_SSH_KEY_PATH", "/path/to/key");

        let config = SshConfig::from_env().expect("Config should be created");

        assert_eq!(config.remote_host, "my-server.com");
        assert_eq!(config.remote_user, "ubuntu");
        assert_eq!(config.local_port, 12345);
        assert_eq!(config.remote_port, 2222);
        assert_eq!(config.bind_port, 6000);
        assert_eq!(config.key_path.unwrap(), "/path/to/key");

        clear_ssh_env();
    }
}
