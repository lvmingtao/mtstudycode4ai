#!/usr/bin/env python3
"""MCP stdio 代理：双向透明转发 Cline(客户端) <-> weather MCP Server 的 JSON-RPC 报文，
并把每一次收发完整记录到 weather_mcp.log。

用法:
    python mcp_proxy.py [--log PATH] [-- <启动 server 的命令...>]

不带命令时默认执行:
    uv --directory <本文件所在目录> run weather.py

日志行格式（按时间顺序追加）:
    2026-09-20T11:05:03.123+08:00 C->S {"jsonrpc":"2.0","id":1,"method":"initialize",...}
    2026-09-20T11:05:03.456+08:00 S->C {"jsonrpc":"2.0","id":1,"result":{...}}
其中 C->S 为客户端发往 server，S->C 为 server 返回客户端。

设计要点:
    1. 只做字节透传，不解析、不修改 JSON-RPC 报文，协议层零侵入;
    2. 日志只写文件与 stderr，绝不写入 stdout（stdout 是 MCP 协议专用通道）;
    3. server 的 stderr 直接继承，服务端异常堆栈仍会出现在 Cline 的 MCP 日志里。
"""

from __future__ import annotations

import argparse
import datetime
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import BinaryIO, Callable

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_LOG = PROJECT_DIR / "weather_mcp.log"
DEFAULT_SERVER_CMD = ["uv", "--directory", str(PROJECT_DIR), "run", "weather.py"]


class TrafficLogger:
    """线程安全地把双向报文追加写入日志文件。"""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._fh = path.open("a", encoding="utf-8", buffering=1)
        self._lock = threading.Lock()

    @staticmethod
    def _stamp() -> str:
        return datetime.datetime.now().astimezone().isoformat(timespec="milliseconds")

    def log(self, direction: str, raw: bytes) -> None:
        """记录一条完整报文（去掉行尾换行）。"""
        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        with self._lock:
            self._fh.write(f"{self._stamp()} {direction} {text}\n")
            self._fh.flush()

    def note(self, text: str) -> None:
        """记录一条分隔性事件（启动/退出等），便于阅读日志。"""
        with self._lock:
            self._fh.write(f"{self._stamp()} ---- {text}\n")
            self._fh.flush()

    def close(self) -> None:
        with self._lock:
            try:
                self._fh.close()
            except Exception:
                pass


def pump(
    src: BinaryIO,
    dst: BinaryIO,
    direction: str,
    log: TrafficLogger,
    on_eof: Callable[[], None],
) -> None:
    """逐行把 src 转发到 dst 并写日志；到 EOF 或管道断开时回调 on_eof。"""
    try:
        while True:
            line = src.readline()
            if not line:
                break
            log.log(direction, line)
            dst.write(line)
            dst.flush()
    except (BrokenPipeError, ConnectionResetError, ValueError, OSError):
        pass  # 任一端关闭都视为会话结束，无需报错
    finally:
        on_eof()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Log the JSON-RPC traffic between an MCP client and the weather MCP server."
    )
    parser.add_argument(
        "--log",
        default=os.environ.get("WEATHER_MCP_LOG") or str(DEFAULT_LOG),
        help=f"日志文件路径（默认 {DEFAULT_LOG}，也可用环境变量 WEATHER_MCP_LOG 指定）",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="'--' 之后的 server 启动命令（缺省则使用 uv run weather.py）",
    )
    args = parser.parse_args()

    cmd = [part for part in args.command if part != "--"] or list(DEFAULT_SERVER_CMD)
    log_path = Path(args.log).expanduser()
    log = TrafficLogger(log_path)

    log.note(
        f"proxy start | pid={os.getpid()} parent_pid={os.getppid()} | server_cmd={' '.join(cmd)}"
    )
    print(f"[mcp-proxy] logging traffic to {log_path}", file=sys.stderr, flush=True)

    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=str(PROJECT_DIR),
            bufsize=0,
        )
    except FileNotFoundError as exc:
        log.note(f"failed to start server: {exc}")
        log.close()
        print(f"[mcp-proxy] cannot start server: {exc}", file=sys.stderr, flush=True)
        return 127

    def stop_child(signum: int, _frame: object) -> None:
        log.note(f"proxy received signal {signum}, terminating server")
        proc.terminate()
        raise SystemExit(0)

    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, stop_child)

    def close_child_stdin() -> None:
        try:
            proc.stdin.close()
        except Exception:
            pass

    # 客户端 -> server 的转发放在后台线程；server -> 客户端 在主线程，直到 server 退出
    client_thread = threading.Thread(
        target=pump,
        args=(sys.stdin.buffer, proc.stdin, "C->S", log, close_child_stdin),
        name="proxy-c2s",
        daemon=True,
    )
    client_thread.start()

    pump(proc.stdout, sys.stdout.buffer, "S->C", log, lambda: None)

    try:
        returncode = proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        returncode = proc.wait()
    client_thread.join(timeout=2)

    log.note(f"server exited with code {returncode}")
    log.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
