"""在文件描述符层面截获 MCP stdio 双向报文，并记录到 weather_mcp.log。

为什么要在 fd 层做:
    mcp 2.x 的 stdio_server() 会接管进程的 fd 0/1 —— 它先把 fd 0 指向 /dev/null、
    把 fd 1 指向 stderr，再用私有的 dup 副本读写 wire。因此在 Python 层包装
    sys.stdin / sys.stdout 是无效的（会被绕过）。这里下沉一层：
        Cline --> fd0(原始) --> [记录线程] --> 自建管道 --> stdio_server 读取
        stdio_server 写 --> 自建管道 --> [记录线程] --> fd1(原始) --> Cline

设计要点:
    1. 字节级透传，不解析、不修改任何 JSON-RPC 报文，协议零侵入;
    2. 日志只写文件，绝不写入 stdout（stdout 是 MCP 协议专用通道）;
    3. 必须在 mcp.run() 之前调用 install();
    4. 日志路径优先取参数，其次环境变量 WEATHER_MCP_LOG，默认与 weather.py 同目录的 weather_mcp.log。
"""

from __future__ import annotations

import datetime
import os
import threading
from pathlib import Path
from typing import Callable

DEFAULT_LOG_NAME = "weather_mcp.log"

_installed = False


def _timestamp() -> str:
    return datetime.datetime.now().astimezone().isoformat(timespec="milliseconds")


def _close_fd(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


def _write_all(fd: int, data: bytes) -> None:
    """把 data 完整写入 fd（管道写可能部分完成，需要循环）。"""
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


class _LineSplitter:
    """把连续的字节流按行切开，保证日志里一条 JSON-RPC 消息占一行。"""

    def __init__(self, emit: Callable[[bytes], None]) -> None:
        self._emit = emit
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> None:
        self._buffer.extend(chunk)
        while True:
            index = self._buffer.find(b"\n")
            if index < 0:
                break
            line = bytes(self._buffer[:index])
            del self._buffer[: index + 1]
            if line.strip():
                self._emit(line)

    def flush(self) -> None:
        """会话结束时把没有换行结尾的残留内容也记录下来。"""
        if self._buffer.strip():
            self._emit(bytes(self._buffer))
        self._buffer.clear()


class _Recorder:
    """线程安全地把报文追加写入日志文件。"""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._fh = path.open("a", encoding="utf-8", buffering=1)
        self._lock = threading.Lock()

    def message(self, direction: str, raw: bytes) -> None:
        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        with self._lock:
            self._fh.write(f"{_timestamp()} {direction} {text}\n")
            self._fh.flush()

    def note(self, text: str) -> None:
        with self._lock:
            self._fh.write(f"{_timestamp()} ---- {text}\n")
            self._fh.flush()


def install(log_path: str | os.PathLike[str] | None = None) -> Path:
    """安装 stdio 截获层，返回实际使用的日志文件路径。

    必须在 mcp.run(transport="stdio") 之前调用；重复调用只有第一次生效。
    """
    global _installed

    resolved = Path(
        log_path
        or os.environ.get("WEATHER_MCP_LOG")
        or Path(__file__).resolve().parent / DEFAULT_LOG_NAME
    ).expanduser()

    if _installed:
        return resolved
    _installed = True

    recorder = _Recorder(resolved)
    recorder.note(f"session start | pid={os.getpid()} ppid={os.getppid()}")

    # 客户端 -> server：真实读端挪到 real_stdin，fd 0 换成自建管道的读端
    real_stdin = os.dup(0)
    in_read, in_write = os.pipe()
    os.dup2(in_read, 0)
    _close_fd(in_read)

    # server -> 客户端：真实写端挪到 real_stdout，fd 1 换成自建管道的写端
    real_stdout = os.dup(1)
    out_read, out_write = os.pipe()
    os.dup2(out_write, 1)
    _close_fd(out_write)

    def forward_client_to_server() -> None:
        splitter = _LineSplitter(lambda line: recorder.message("C->S", line))
        try:
            while True:
                chunk = os.read(real_stdin, 65536)
                if not chunk:
                    break
                splitter.feed(chunk)
                _write_all(in_write, chunk)
        except OSError:
            pass
        finally:
            splitter.flush()
            _close_fd(in_write)
            recorder.note("client closed stdin, C->S finished")

    def forward_server_to_client() -> None:
        splitter = _LineSplitter(lambda line: recorder.message("S->C", line))
        try:
            while True:
                chunk = os.read(out_read, 65536)
                if not chunk:
                    break
                splitter.feed(chunk)
                _write_all(real_stdout, chunk)
        except OSError:
            pass
        finally:
            splitter.flush()
            recorder.note("server closed stdout, S->C finished")

    threading.Thread(target=forward_client_to_server, name="mcp-log-c2s", daemon=True).start()
    threading.Thread(target=forward_server_to_client, name="mcp-log-s2c", daemon=True).start()

    return resolved
