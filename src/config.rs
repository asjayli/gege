use std::env;

/// 危险环境变量黑名单：禁止从外部请求注入到子进程中
pub const DANGEROUS_ENV_VARS: &[&str] = &[
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "DYLD_INSERT_LIBRARIES",
    "DYLD_LIBRARY_PATH",
    "PATH",
    "SHELL",
    "IFS",
    "PYTHONPATH",
    "PERLLIB",
    "PERL5LIB",
    "CLASSPATH",
    "JAVA_HOME",
    "NODE_PATH",
    "RUBYLIB",
    "GEM_PATH",
    "RUSTFLAGS",
    "HOME",
    "USER",
    "LOGNAME",
    "TMPDIR",
    "HOSTNAME",
];

pub struct GegeConfig {
    pub auth_token: String,
    pub local_port: u16,
    pub http_port: u16,
}

impl GegeConfig {
    pub fn from_env() -> Self {
        let auth_token = env::var("GEGE_AUTH_TOKEN")
            .expect("GEGE_AUTH_TOKEN must be set in environment");

        let local_port: u16 = env::var("GEGE_LOCAL_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50051);

        let http_port: u16 = env::var("GEGE_HTTP_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8081);

        assert!(local_port != 0, "GEGE_LOCAL_PORT must not be 0");
        assert!(http_port != 0, "GEGE_HTTP_PORT must not be 0");
        assert!(local_port != http_port, "GEGE_LOCAL_PORT and GEGE_HTTP_PORT must be different");

        Self {
            auth_token,
            local_port,
            http_port,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static CONFIG_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn clear_config_env() {
        env::remove_var("GEGE_AUTH_TOKEN");
        env::remove_var("GEGE_LOCAL_PORT");
        env::remove_var("GEGE_HTTP_PORT");
    }

    #[test]
    fn test_dangerous_env_vars_contains_common_keys() {
        assert!(DANGEROUS_ENV_VARS.contains(&"LD_PRELOAD"));
        assert!(DANGEROUS_ENV_VARS.contains(&"PATH"));
        assert!(DANGEROUS_ENV_VARS.contains(&"SHELL"));
        assert!(DANGEROUS_ENV_VARS.contains(&"HOME"));
        assert!(!DANGEROUS_ENV_VARS.contains(&"SAFE_VAR"));
    }

    #[test]
    fn test_gege_config_from_env_defaults() {
        let _lock = CONFIG_TEST_LOCK.lock().unwrap();
        clear_config_env();
        env::set_var("GEGE_AUTH_TOKEN", "test-token-123");

        let config = GegeConfig::from_env();
        assert_eq!(config.auth_token, "test-token-123");
        assert_eq!(config.local_port, 50051);
        assert_eq!(config.http_port, 8081);

        clear_config_env();
    }

    #[test]
    fn test_gege_config_from_env_custom_ports() {
        let _lock = CONFIG_TEST_LOCK.lock().unwrap();
        clear_config_env();
        env::set_var("GEGE_AUTH_TOKEN", "token");
        env::set_var("GEGE_LOCAL_PORT", "60001");
        env::set_var("GEGE_HTTP_PORT", "9000");

        let config = GegeConfig::from_env();
        assert_eq!(config.local_port, 60001);
        assert_eq!(config.http_port, 9000);

        clear_config_env();
    }

    #[test]
    fn test_gege_config_missing_auth_token_panics() {
        let _lock = CONFIG_TEST_LOCK.lock().unwrap();
        clear_config_env();
        let result = std::panic::catch_unwind(|| {
            let _ = GegeConfig::from_env();
        });
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = if let Some(s) = err.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = err.downcast_ref::<String>() {
            s.clone()
        } else {
            String::new()
        };
        assert!(msg.contains("GEGE_AUTH_TOKEN must be set in environment"));
    }

    #[test]
    fn test_gege_config_local_port_zero_panics() {
        let _lock = CONFIG_TEST_LOCK.lock().unwrap();
        clear_config_env();
        env::set_var("GEGE_AUTH_TOKEN", "token");
        env::set_var("GEGE_LOCAL_PORT", "0");
        let result = std::panic::catch_unwind(|| {
            let _ = GegeConfig::from_env();
        });
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = if let Some(s) = err.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = err.downcast_ref::<String>() {
            s.clone()
        } else {
            String::new()
        };
        assert!(msg.contains("GEGE_LOCAL_PORT must not be 0"));
    }

    #[test]
    fn test_gege_config_http_port_zero_panics() {
        let _lock = CONFIG_TEST_LOCK.lock().unwrap();
        clear_config_env();
        env::set_var("GEGE_AUTH_TOKEN", "token");
        env::set_var("GEGE_HTTP_PORT", "0");
        let result = std::panic::catch_unwind(|| {
            let _ = GegeConfig::from_env();
        });
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = if let Some(s) = err.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = err.downcast_ref::<String>() {
            s.clone()
        } else {
            String::new()
        };
        assert!(msg.contains("GEGE_HTTP_PORT must not be 0"));
    }

    #[test]
    fn test_gege_config_same_port_panics() {
        let _lock = CONFIG_TEST_LOCK.lock().unwrap();
        clear_config_env();
        env::set_var("GEGE_AUTH_TOKEN", "token");
        env::set_var("GEGE_LOCAL_PORT", "8080");
        env::set_var("GEGE_HTTP_PORT", "8080");
        let result = std::panic::catch_unwind(|| {
            let _ = GegeConfig::from_env();
        });
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = if let Some(s) = err.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = err.downcast_ref::<String>() {
            s.clone()
        } else {
            String::new()
        };
        assert!(msg.contains("GEGE_LOCAL_PORT and GEGE_HTTP_PORT must be different"));
    }
}
