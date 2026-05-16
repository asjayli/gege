# Gege 代理层集成测试指导方案 (Integration Test Strategy)

由于 Gege 是一个作为纯粹底层中间件存在的代理层，它的集成测试不能仅仅局限于内部的函数调用，而应当覆盖网络层 (HTTP/gRPC) 和子进程 (Executors) 的调度行为。以下是编写和执行集成测试的指导方案：

## 1. 测试环境准备

集成测试应该在脱离线上生产的独立环境中进行，或者在沙箱（Sandbox）内启动测试服务。

- **推荐工具**: Rust 官方的 `cargo test` 配合 `tokio::test` 或外部脚本 (比如 Python `pytest` / shell `bash`)。
- **Mock 环境**: 
  - 不要直接执行真实的大模型 CLI（例如 `claude`, `gemini`），这些可能会产生费用且速度慢、极其不稳定。
  - **建议做法**: 在测试机器上配置一个名为 `claude` / `gemini` 的假可执行脚本（例如使用 `bash` 编写的 mock），或者通过 `env_vars` 将实际命令指向本地的 Mock 二进制文件。

## 2. 核心场景覆盖 (Test Cases)

### 场景一：gRPC 端到端长连接测试 (ExecuteTaskStream)
**目的**: 验证 Java 等客户端能否通过 gRPC 成功提交流式任务，并接收到底层进程输出的 stdout/stderr。
**测试步骤**:
1. 启动 Gege 服务（使用一个非标准测试端口，例如 `50052`）。
2. 使用 gRPC 客户端连接该端口。
3. 提交 `ExecuteTaskStream`，指定 `AgentType::ClaudeCode`，并在 `env_vars` 注入环境变量将底层执行脚本替换为 `echo "test out" && sleep 1 && >&2 echo "test err"`。
4. **断言**: 客户端 Stream 收到两次 `TaskResponse`，且内容分别包含 `OUT: test out` 和 `ERR: test err`。状态以 `RUNNING` 变为 `COMPLETED` 结束。

### 场景二：HTTP Webhook 回调与多租户隔离 (SubmitTask)
**目的**: 验证 Gege 能否正确解耦发送任务，并在结束时向正确的 `callback_url` 主动推流，携带正确的 Header 与格式。
**测试步骤**:
1. 启动一个本地的 Mock HTTP Server (例如 `axum` test server) 作为目标 Webhook 接收端。
2. 调用 Gege 的 `/v1/tasks/submit` 提交任务，并配置 `callback_url` 为刚刚启动的 Mock Server，配置 `callback_format` 为 `FEISHU_BOT`，并携带自定义的鉴权 Header。
3. 等待 Mock Executor 退出。
4. **断言**: Mock Server 成功接收到了包含飞书标准格式 `{"msg_type": "text", ...}` 的 JSON 数据，且 HTTP Headers 中包含了传递的自定义键值对。

### 场景三：SSH 反向隧道保活与重连
**目的**: 验证内网机器能在不稳定网络下保持 Remote Control 连通性。
**测试步骤**:
1. 启动本地 Mock SSHD 服务监听 `2222` 端口。
2. 注入环境变量 `GEGE_SSH_REMOTE_HOST=127.0.0.1`, `GEGE_SSH_REMOTE_PORT=2222` 启动 Gege。
3. 人为强杀本地的 Mock SSHD 服务以切断连接。
4. **断言**: Gege 不崩溃，且能在等待数秒后尝试重连 SSHD 服务。

### 场景四：防卡死安全控制 (Timeout Killing)
**目的**: 验证设置了超时参数的任务，不会导致 Gege 出现僵尸进程。
**测试步骤**:
1. 使用 `/v1/tasks/submit` 提交任务，执行一个带有无限循环的死锁 Shell (`sleep 9999`)，并设定 `timeout_seconds = 2`。
2. 等待 2.5 秒。
3. 调用 `/v1/tasks/{task_id}/status`。
4. **断言**: 状态为 `TIMEOUT`，且系统级查询不到原本由于 `sleep 9999` 启动的子进程（被成功 Kill）。

## 3. 集成测试自动化 (CI/CD)

在代码库的 `.github/workflows` 或者您的内部 CI 平台中增加以下流程：

```yaml
test:
  steps:
    - name: Setup Mock CLI tools
      run: |
        echo '#!/bin/bash' > /tmp/claude
        echo 'echo "mock success"' >> /tmp/claude
        chmod +x /tmp/claude
        export PATH="/tmp:$PATH"
        
    - name: Run Gege Integration Tests
      run: cargo test --test integration_tests
```

您可以根据上述指导，在 `tests/integration_tests.rs` 中使用 `reqwest` 和 `tonic` 的 Client 构建自动化脚本。