# Gege (AI Agent 代理层)

Gege 是一个高性能、轻量级的本地 Rust 代理层。它的核心使命是将 Java 工作流引擎等后端控制端和各种本地/云端 AI CLI 执行环境（如 Claude Code, Gemini CLI, Hehe 等）解耦。

## 核心特性

- **协议双支持**：支持 `ExecuteTaskStream`（长连接流式日志）和 `SubmitTask`（Fire & Forget 异步回调），适应各种持久化和网络条件。
- **环境安全**：隔离运行空间 (`workspace_dir`) 并且支持注入环境变量与 `auth_token`。
- **内网穿透 / 远程控制 (SSH Tunnel)**：内置 SSH 隧道支持。可以将本地绑定的 gRPC 服务反向暴露到拥有公网 IP 的堡垒机上，轻松对接飞书、钉钉、企业微信机器人等 Webhook 或远端 Actuator。

## 构建与运行

确保安装了 Rust 工具链 (支持 2021 edition)：

```bash
cargo build --release
```

可以直接运行（默认监听本地 `127.0.0.1:50051`）：

```bash
cargo run
```

## 远程控制 (Remote Control) 配置

如果你想要远端的 Java 进程控制你本地机器上的 Gege（例如将你的笔记本作为 Worker），可以通过配置环境变量激活内置的 SSH 反向隧道。Gege 启动时会在后台自动拉起并守护 `ssh -R` 进程：

```bash
# 开启远程穿透的前提：配置远端主机地址
export GEGE_SSH_REMOTE_HOST="your-remote-server.com"

# [可选] 远端 SSH 端口，默认 22
export GEGE_SSH_REMOTE_PORT="22"

# [可选] SSH 登录用户名，默认 root
export GEGE_SSH_USER="ubuntu"

# [可选] 映射到远端服务器的端口，Java 将连接远端的此端口，默认 50051
export GEGE_SSH_BIND_PORT="50051"

# [可选] Gege 在本地实际监听的端口，默认 50051
export GEGE_LOCAL_PORT="50051"

# [可选] SSH 密钥文件路径 (免密登录)，不填则依赖系统默认的 ssh-agent
export GEGE_SSH_KEY_PATH="~/.ssh/id_rsa"

# 启动
cargo run
```

**运行效果**：
1. Gege 本地监听 `127.0.0.1:50051`
2. 后台自动执行：`ssh -N -R 50051:127.0.0.1:50051 ubuntu@your-remote-server.com -p 22 -i ~/.ssh/id_rsa ...`
3. 云端的 Java 只需要通过 gRPC 连接 `127.0.0.1:50051` (相对云端服务器而言)，流量就会走加密隧道送到你本地的 Gege 执行！

> 注意：需要在远端服务器的 `/etc/ssh/sshd_config` 中确保 `GatewayPorts yes` 已开启，或者 Java 与 sshd 在同一台服务器内部互通。

## 鉴权

在 `TaskRequest` 中可以配置 `auth_token`，默认演示用的硬编码 Token 为 `hehe-super-secret-token`。
当 Java 端使用 `SubmitTask` 模式并携带 `callback_url`，Gege 会在使用本机的网络（不走 SSH 隧道）将执行状态和结果通过 HTTP POST 回调给您的 Java Webhook 接口。

## 参与贡献

1. Fork 本仓库
2. 新建 feat_xxx 分支
3. 提交代码
4. 新建 Pull Request
