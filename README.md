# mtstudycode4ai

## mcp-cline

采用 cline 进行mcp实践学习

### 采用uv命令创建python工程 weather

```shell
~$ uv init weather
Initialized project weather at <directory>/weather
~$ cd weather
~/weather$ uv venv
Using CPython 3.14.7
Creating virtual environment at: .venv
Activate with: source .venv/bin/activate
~/weather$ source .venv/bin/activate
~/weather$ uv add "mcp[cli]" httpx
Resolved 43 packages in 2.10s
      Built weather @ file:///~/weather                                                                 Prepared 14 packages in 1.20s
░░░░░░░░░░░░░░░░░░░░ [0/40] Installing wheels...                                                                                                       warning: Failed to hardlink files; falling back to full copy. This may lead to degraded performance.
         If the cache and target directories are on different filesystems, hardlinking may not be supported.
         If this is intentional, set `export UV_LINK_MODE=copy` or use `--link-mode=copy` to suppress this warning.
Installed 40 packages in 512ms
...
```

###  注册weather为mcpServer

将该脚本注册到mcpserver中，cline的是 ~/.cline/data/settings/cline_mcp_settings.json

```json
{
  "mcpServers": {
    "weather": {
      "disabled": false,
      "timeout": 30,
      "args": [
        "--directory",
        "~/weather",
        "run",
        "weather.py"
      ],
      "transportType": "stdio",
      "command": "uv"
    }
  }
}
```

### 查看mcp与cline通信的日志

编写如下脚本 mcp_traffic_log.py

### 查看cline与deepseek通信的日志

编写如下脚本 llm_logger.py

```shell
cd mcp/cline-llm
source .venv/bin/activate
python llm_logger.py          # 监听 127.0.0.1:8000，转发到 https://api.deepseek.com，日志写入 cline_llm.log
```

然后把 Cline 中 DeepSeek 的 Base URL 改成 `http://127.0.0.1:8000/v1`（API Key 仍填 DeepSeek 的 Key），
Cline 与 DeepSeek 之间的每一次请求/响应都会写入 `mcp/cline-llm/cline_llm.log`：

```
2026-09-20T15:55:52.237+08:00 ---- session start | pid=90166 | listen=127.0.0.1:8000 | upstream=https://api.deepseek.com
2026-09-20T15:56:46.739+08:00 ---- C->S POST /v1/chat/completions -> https://api.deepseek.com/v1/chat/completions | auth=Bearer sk-***
2026-09-20T15:56:46.739+08:00 C->S {"model":"deepseek-chat","messages":[...],"stream":true}
2026-09-20T15:56:47.013+08:00 ---- S->C status=200 content-type=text/event-stream; charset=utf-8
2026-09-20T15:56:47.014+08:00 S->C data: {"id":"...","choices":[...]}
2026-09-20T15:56:48.100+08:00 S->C data: [DONE]
```

其中 `C->S` 是 Cline 发给 DeepSeek 的内容，`S->C` 是 DeepSeek 返回给 Cline 的内容，`----` 开头的是事件行。

可配置项（命令行参数优先，其次环境变量）：

| 命令行参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--host` | `CLINE_LLM_HOST` | `127.0.0.1` | 监听地址，改成 `0.0.0.0` 可被其他主机访问（API Key 会一并暴露，谨慎） |
| `--port` | `CLINE_LLM_PORT` | `8000` | 监听端口 |
| `--log` | `CLINE_LLM_LOG` | 脚本同目录的 `cline_llm.log` | 日志文件路径（追加写入，不清空） |
| `--upstream` | `LLM_UPSTREAM_BASE` | `https://api.deepseek.com` | 上游地址，不含 `/v1` |

说明：
- 代理只做透明转发，不改写路径与请求体，`/v1/chat/completions`、`/chat/completions`、`/v1/models` 等都能直接透传；
- 流式(SSE)响应边收边发、不缓冲，Cline 的实时输出不受影响；
- 日志中的 `Authorization` 只保留前 10 个字符，避免把 API Key 明文写入日志；
- 可用 `curl http://127.0.0.1:8000/healthz` 确认代理是否在运行。
