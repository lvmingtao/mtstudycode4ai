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