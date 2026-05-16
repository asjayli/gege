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
        let local_port = env::var("GEGE_LOCAL_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50051);
        let http_port = env::var("GEGE_HTTP_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8081);
        Self {
            auth_token,
            local_port,
            http_port,
        }
    }
}
