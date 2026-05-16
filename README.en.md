# Gege (AI Agent Proxy Layer)

Gege is a high-performance, lightweight local Rust proxy layer. Its core mission is to decouple backend controllers (such as Java workflow engines) from various local/cloud AI CLI execution environments (e.g., Claude Code, Gemini CLI, Hehe, etc.).

## Core Features

- **Dual Protocol Support**: Supports both `ExecuteTaskStream` (long-lived streaming logs) and `SubmitTask` (fire-and-forget async callback), adapting to various persistence and network conditions.
- **Environment Security**: Isolated execution space (`workspace_dir`) with support for injecting environment variables and `auth_token`.
- **NAT Traversal / Remote Control (SSH Tunnel)**: Built-in SSH tunnel support. Expose locally-bound gRPC services to a bastion host with a public IP, enabling easy integration with Feishu, DingTalk, WeCom bots, webhooks, or remote actuators.

## Build & Run

Make sure you have the Rust toolchain installed (2021 edition):

```bash
cargo build --release
```

Run directly (listens on `127.0.0.1:50051` by default):

```bash
cargo run
```

## Remote Control Configuration

If you want a remote Java process to control Gege on your local machine (e.g., using your laptop as a Worker), you can activate the built-in SSH reverse tunnel via environment variables. Gege will automatically launch and daemonize an `ssh -R` process on startup:

```bash
# Prerequisite: configure the remote host address
export GEGE_SSH_REMOTE_HOST="your-remote-server.com"

# [Optional] Remote SSH port, default 22
export GEGE_SSH_REMOTE_PORT="22"

# [Optional] SSH username, default root
export GEGE_SSH_USER="ubuntu"

# [Optional] Port mapped on the remote server, default 50051
export GEGE_SSH_BIND_PORT="50051"

# [Optional] Local port Gege listens on, default 50051
export GEGE_LOCAL_PORT="50051"

# [Optional] SSH key file path (passwordless login), defaults to system ssh-agent
export GEGE_SSH_KEY_PATH="~/.ssh/id_rsa"

# Start
cargo run
```

**How it works**:
1. Gege listens locally on `127.0.0.1:50051`
2. Automatically runs in background: `ssh -N -R 50051:127.0.0.1:50051 ubuntu@your-remote-server.com -p 22 -i ~/.ssh/id_rsa ...`
3. The cloud-side Java process connects to `127.0.0.1:50051` (relative to the cloud server), and traffic flows through the encrypted tunnel to your local Gege!

> Note: Ensure `GatewayPorts yes` is enabled in the remote server's `/etc/ssh/sshd_config`, or that Java and sshd can communicate internally on the same server.

## Authentication

You can configure `auth_token` in `TaskRequest`. The default hardcoded demo token is `hehe-super-secret-token`.
When the Java side uses `SubmitTask` mode with a `callback_url`, Gege will POST execution status and results back to your Java webhook endpoint using the local network (not through the SSH tunnel).

## Contributing

1. Fork the repository
2. Create feat_xxx branch
3. Commit your code
4. Create Pull Request
