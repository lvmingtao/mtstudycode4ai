#!/usr/bin/env python3
"""Cline <-> DeepSeek 通信代理：监听 8000 端口做透明转发，并把通信内容写入 cline_llm.log。

链路:
    Cline --(OpenAI 兼容请求)--> http://127.0.0.1:8000 --(原样转发)--> DeepSeek API

用法:
    python llm_logger.py                                   # 监听 127.0.0.1:8000，日志写 cline_llm.log
    python llm_logger.py --host 0.0.0.0                    # 对外暴露（会一并暴露 API Key，谨慎使用）
    python llm_logger.py --log /tmp/cline_llm.log --upstream https://api.deepseek.com

Cline 侧把 DeepSeek 的 Base URL 改成 http://127.0.0.1:8000/v1 即可（API Key 仍填 DeepSeek 的 Key）。
也可以填成 http://127.0.0.1:8000（DeepSeek 同时支持 /chat/completions 与 /v1/chat/completions），
代理按原始路径转发，不对路径、请求体做任何改写。

日志格式（按时间顺序追加，一条记录一行）:
    2026-09-20T15:30:00.000+08:00 ---- session start | pid=123 | listen=127.0.0.1:8000 | upstream=https://api.deepseek.com
    2026-09-20T15:30:01.001+08:00 ---- C->S POST /v1/chat/completions -> https://api.deepseek.com/v1/chat/completions | auth=Bearer sk-***
    2026-09-20T15:30:01.002+08:00 C->S {"model":"deepseek-chat","messages":[...],"stream":true}
    2026-09-20T15:30:01.050+08:00 ---- S->C status=200 content-type=text/event-stream
    2026-09-20T15:30:01.060+08:00 S->C data: {"id":"...","choices":[...]}
    2026-09-20T15:30:02.100+08:00 S->C data: [DONE]
其中 C->S 为 Cline 发往 DeepSeek，S->C 为 DeepSeek 返回 Cline；---- 开头的是事件行。

设计要点:
    1. 路径、查询串、请求体、状态码与响应头原样透传（只剔除逐跳头部，长度/编码交给 httpx 与服务端重算）;
    2. 流式(SSE)响应边收边发、不缓冲，保证 Cline 的实时输出不被破坏，同时按行落日志;
    3. 日志追加写入、启动不清空，用 "---- session start" 行分隔每次运行;
    4. Authorization 只记录前若干字符，避免把 API Key 明文写进日志。
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Callable

import httpx
from fastapi import FastAPI, Request
from starlette.responses import Response, StreamingResponse

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_LOG = PROJECT_DIR / "cline_llm.log"
DEFAULT_UPSTREAM_BASE = "https://api.deepseek.com"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000

# 逐跳头部：不能透传，长度/编码交给 httpx 与本服务重算
REQUEST_DROP_HEADERS = {
    "accept-encoding",
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
RESPONSE_DROP_HEADERS = {
    "connection",
    "content-encoding",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

# 运行期配置（main() 中可被命令行参数覆盖）
host = os.environ.get("CLINE_LLM_HOST") or DEFAULT_HOST
port = int(os.environ.get("CLINE_LLM_PORT") or DEFAULT_PORT)
log_path = Path(os.environ.get("CLINE_LLM_LOG") or DEFAULT_LOG).expanduser()


def _legacy_upstream_base() -> str | None:
    """兼容旧变量 LLM_UPSTREAM_URL（原先指向完整的 /chat/completions 地址）。"""
    url = os.environ.get("LLM_UPSTREAM_URL")
    if not url:
        return None
    suffix = "/chat/completions"
    return url[: -len(suffix)] if url.endswith(suffix) else url


upstream_base = (
    os.environ.get("LLM_UPSTREAM_BASE") or _legacy_upstream_base() or DEFAULT_UPSTREAM_BASE
).rstrip("/")


def _timestamp() -> str:
    return datetime.datetime.now().astimezone().isoformat(timespec="milliseconds")


def _mask_secret(value: str | None, keep: int = 10) -> str:
    """只保留密钥的前 keep 个字符，其余用 *** 代替。"""
    if not value:
        return "-"
    return value if len(value) <= keep else f"{value[:keep]}***"


def _single_line(raw: bytes) -> str:
    """把报文压成单行文本，保证一条记录只占日志的一行。"""
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return "<empty>"
    try:
        payload = json.loads(text)
    except ValueError:
        return text.replace("\r\n", "\\n").replace("\n", "\\n")
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


class TrafficLogger:
    """线程安全地把通信内容追加写入日志文件（文件句柄懒打开，便于构造后再改路径）。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh = None
        self._lock = threading.Lock()

    def _handle(self):
        if self._fh is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = self.path.open("a", encoding="utf-8", buffering=1)
        return self._fh

    def _write(self, text: str) -> None:
        with self._lock:
            handle = self._handle()
            handle.write(f"{_timestamp()} {text}\n")
            handle.flush()

    def note(self, text: str) -> None:
        """记录一条事件（启动、状态码、异常等）。"""
        self._write(f"---- {text}")

    def log(self, direction: str, text: str) -> None:
        """记录一条报文，direction 为 C->S 或 S->C。"""
        self._write(f"{direction} {text}")

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.close()
                finally:
                    self._fh = None


class LineSplitter:
    """把连续的字节流按行切开，保证日志里一条 SSE 事件占一行。"""

    def __init__(self, emit: Callable[[bytes], None]) -> None:
        self._emit = emit
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> None:
        self._buffer.extend(chunk)
        while True:
            index = self._buffer.find(b"\n")
            if index < 0:
                break
            line = bytes(self._buffer[:index]).rstrip(b"\r")
            del self._buffer[: index + 1]
            if line.strip():
                self._emit(line)

    def flush(self) -> None:
        """会话结束时把没有换行结尾的残留内容也记录下来。"""
        if self._buffer.strip():
            self._emit(bytes(self._buffer).rstrip(b"\r"))
        self._buffer.clear()


logger = TrafficLogger(log_path)
http_client: httpx.AsyncClient

# 不设 read 超时：模型生成过程中可能长时间没有数据
HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=None, write=60.0, pool=15.0)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=False)
    logger.note(
        f"session start | pid={os.getpid()} | listen={host}:{port} "
        f"| upstream={upstream_base} | log={logger.path}"
    )
    try:
        yield
    finally:
        await http_client.aclose()
        logger.note("session stop")
        logger.close()


app = FastAPI(
    title="Cline LLM Traffic Logger",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)


async def _relay(upstream_response: httpx.Response, is_stream: bool) -> AsyncIterator[bytes]:
    """把上游响应转发给 Cline，同时写入日志；任一端断开都会关闭上游连接。"""
    if is_stream:
        splitter = LineSplitter(lambda line: logger.log("S->C", _single_line(line)))
        try:
            async for chunk in upstream_response.aiter_bytes():
                splitter.feed(chunk)
                yield chunk
        finally:
            splitter.flush()
            await upstream_response.aclose()
        return

    try:
        raw = await upstream_response.aread()
        logger.log("S->C", _single_line(raw))
        if raw:
            yield raw
    finally:
        await upstream_response.aclose()


@app.get("/healthz")
async def healthz() -> dict:
    """本地健康检查，不转发到上游，便于确认代理是否在运行。"""
    return {"status": "ok", "upstream": upstream_base, "log": str(logger.path)}


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def proxy_request(request: Request, path: str):
    """把请求原样转发到上游 DeepSeek，并把请求体与响应体写入 cline_llm.log。"""
    body = await request.body()
    query = request.url.query
    target = f"{upstream_base}/{path.lstrip('/')}" if path else upstream_base
    if query:
        target = f"{target}?{query}"

    logger.note(
        f"C->S {request.method} /{path}{'?' + query if query else ''} -> {target} "
        f"| auth={_mask_secret(request.headers.get('authorization'))}"
    )
    if body:
        logger.log("C->S", _single_line(body))

    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in REQUEST_DROP_HEADERS
    }

    try:
        upstream_request = http_client.build_request(
            request.method, target, headers=headers, content=body
        )
        upstream_response = await http_client.send(upstream_request, stream=True)
    except httpx.HTTPError as exc:
        logger.note(f"!! upstream request failed: {exc!r}")
        return Response(
            content=json.dumps(
                {"error": {"message": f"cline-llm proxy: 无法访问上游 {target}: {exc}"}},
                ensure_ascii=False,
            ),
            status_code=502,
            media_type="application/json",
        )

    content_type = upstream_response.headers.get("content-type", "")
    is_stream = "text/event-stream" in content_type.lower()
    logger.note(f"S->C status={upstream_response.status_code} content-type={content_type or '-'}")

    response_headers = {
        key: value
        for key, value in upstream_response.headers.items()
        if key.lower() not in RESPONSE_DROP_HEADERS
    }

    return StreamingResponse(
        _relay(upstream_response, is_stream),
        status_code=upstream_response.status_code,
        headers=response_headers,
        media_type=None,
    )


def main() -> int:
    global logger, log_path, upstream_base, host, port

    parser = argparse.ArgumentParser(
        description="代理 Cline 与 DeepSeek 之间的通信，并把通信内容写入日志文件。"
    )
    parser.add_argument(
        "--host",
        default=host,
        help=f"监听地址（默认 {DEFAULT_HOST}，也可用环境变量 CLINE_LLM_HOST 指定）",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=port,
        help=f"监听端口（默认 {DEFAULT_PORT}，也可用环境变量 CLINE_LLM_PORT 指定）",
    )
    parser.add_argument(
        "--log",
        default=str(log_path),
        help=f"日志文件路径（默认 {DEFAULT_LOG}，也可用环境变量 CLINE_LLM_LOG 指定）",
    )
    parser.add_argument(
        "--upstream",
        default=upstream_base,
        help=f"上游地址（默认 {DEFAULT_UPSTREAM_BASE}，也可用环境变量 LLM_UPSTREAM_BASE 指定）",
    )
    args = parser.parse_args()

    log_path = Path(args.log).expanduser()
    upstream_base = args.upstream.rstrip("/")
    host, port = args.host, args.port
    logger = TrafficLogger(log_path)

    import uvicorn

    print(
        f"[cline-llm] 监听 {host}:{port}，转发到 {upstream_base}，日志写入 {log_path}",
        flush=True,
    )
    uvicorn.run(app, host=host, port=port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

