"""Minimal RFC6455 client (text frames + ping/pong) using only the stdlib."""

from __future__ import annotations

import base64
import hashlib
import os
import socket
import ssl
import struct
from typing import Optional
from urllib.parse import urlparse, unquote


class WebSocketError(RuntimeError):
    pass


def _normalize_proxy(proxy: Optional[str]) -> Optional[str]:
    if not proxy:
        return None
    value = str(proxy).strip()
    if not value or value.lower() in {"none", "direct", "off", "0"}:
        return None
    if "://" not in value:
        value = "http://" + value
    return value


def _connect_tcp(
    host: str,
    port: int,
    *,
    timeout_s: float,
    proxy: Optional[str] = None,
) -> socket.socket:
    proxy_url = _normalize_proxy(proxy)
    if not proxy_url:
        return socket.create_connection((host, port), timeout=timeout_s)
    parsed = urlparse(proxy_url)
    scheme = (parsed.scheme or "http").lower()
    if scheme not in {"http", "https"}:
        raise WebSocketError("unsupported proxy scheme for websocket: {}".format(scheme))
    proxy_host = parsed.hostname
    if not proxy_host:
        raise WebSocketError("proxy host missing")
    proxy_port = parsed.port or (443 if scheme == "https" else 80)
    sock = socket.create_connection((proxy_host, proxy_port), timeout=timeout_s)
    sock.settimeout(timeout_s)
    target = "{}:{}".format(host, port)
    headers = [
        "CONNECT {} HTTP/1.1".format(target),
        "Host: {}".format(target),
        "Proxy-Connection: keep-alive",
    ]
    if parsed.username is not None:
        token = "{}:{}".format(unquote(parsed.username), unquote(parsed.password or ""))
        headers.append(
            "Proxy-Authorization: Basic {}".format(
                base64.b64encode(token.encode("utf-8")).decode("ascii")
            )
        )
    headers.append("")
    headers.append("")
    sock.sendall("\r\n".join(headers).encode("ascii"))
    response = bytearray()
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            sock.close()
            raise WebSocketError("proxy CONNECT closed")
        response.extend(chunk)
        if len(response) > 65536:
            sock.close()
            raise WebSocketError("proxy CONNECT response too large")
    head = bytes(response).split(b"\r\n\r\n", 1)[0]
    status_line = head.split(b"\r\n", 1)[0].decode("latin1", errors="replace")
    if "200" not in status_line:
        sock.close()
        raise WebSocketError("proxy CONNECT failed: {}".format(status_line))
    return sock


class MinimalWebSocket:
    """Blocking WebSocket for long-lived Synoptic push feeds."""

    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._buf = bytearray()
        self.closed = False

    @classmethod
    def connect(
        cls,
        url: str,
        *,
        timeout_s: float = 30.0,
        origin: Optional[str] = None,
        proxy: Optional[str] = None,
        idle_timeout_s: float = 30.0,
    ) -> "MinimalWebSocket":
        parsed = urlparse(url)
        if parsed.scheme not in {"ws", "wss"}:
            raise WebSocketError("unsupported websocket scheme: {}".format(parsed.scheme))
        host = parsed.hostname or ""
        if not host:
            raise WebSocketError("websocket host missing")
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = path + "?" + parsed.query
        raw = _connect_tcp(host, port, timeout_s=timeout_s, proxy=proxy)
        raw.settimeout(timeout_s)
        sock: socket.socket = raw
        if parsed.scheme == "wss":
            context = ssl.create_default_context()
            sock = context.wrap_socket(raw, server_hostname=host)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        headers = [
            "GET {} HTTP/1.1".format(path),
            "Host: {}".format(host if (parsed.port is None) else "{}:{}".format(host, port)),
            "Upgrade: websocket",
            "Connection: Upgrade",
            "Sec-WebSocket-Key: {}".format(key),
            "Sec-WebSocket-Version: 13",
        ]
        if origin:
            headers.append("Origin: {}".format(origin))
        headers.append("")
        headers.append("")
        sock.sendall("\r\n".join(headers).encode("ascii"))
        response = bytearray()
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                raise WebSocketError("websocket handshake closed")
            response.extend(chunk)
            if len(response) > 65536:
                raise WebSocketError("websocket handshake too large")
        head, _, rest = bytes(response).partition(b"\r\n\r\n")
        status_line = head.split(b"\r\n", 1)[0].decode("latin1", errors="replace")
        if "101" not in status_line:
            raise WebSocketError("websocket handshake failed: {}".format(status_line))
        accept = None
        for line in head.split(b"\r\n")[1:]:
            if b":" not in line:
                continue
            name, value = line.split(b":", 1)
            if name.strip().lower() == b"sec-websocket-accept":
                accept = value.strip().decode("ascii", errors="replace")
                break
        expected = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
        ).decode("ascii")
        if accept != expected:
            raise WebSocketError("websocket accept mismatch")
        client = cls(sock)
        if rest:
            client._buf.extend(rest)
        sock.settimeout(max(5.0, float(idle_timeout_s)))
        return client

    def recv_text(self) -> Optional[str]:
        """Return next text payload, None on clean close, raise on error."""

        while True:
            opcode, payload = self._recv_frame()
            if opcode == 0x8:  # close
                self.closed = True
                try:
                    self._send_frame(0x8, payload[:2] if payload else b"")
                except OSError:
                    pass
                self.close()
                return None
            if opcode == 0x9:  # ping
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:  # pong
                continue
            if opcode == 0x1:
                return payload.decode("utf-8")
            if opcode == 0x2:
                raise WebSocketError("binary websocket frames are not supported")
            raise WebSocketError("unsupported websocket opcode {}".format(opcode))

    def send_text(self, text: str) -> None:
        self._send_frame(0x1, text.encode("utf-8"))

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self._sock.close()
        except OSError:
            pass

    def _recv_exact(self, size: int) -> bytes:
        while len(self._buf) < size:
            chunk = self._sock.recv(max(4096, size - len(self._buf)))
            if not chunk:
                raise WebSocketError("websocket closed while reading")
            self._buf.extend(chunk)
        out = bytes(self._buf[:size])
        del self._buf[:size]
        return out

    def _recv_frame(self) -> tuple[int, bytes]:
        header = self._recv_exact(2)
        b1, b2 = header[0], header[1]
        fin = b1 & 0x80
        opcode = b1 & 0x0F
        masked = b2 & 0x80
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_exact(8))[0]
        mask = self._recv_exact(4) if masked else b""
        payload = self._recv_exact(length) if length else b""
        if masked:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        if not fin:
            raise WebSocketError("fragmented websocket frames are not supported")
        return opcode, payload

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self.closed:
            raise WebSocketError("websocket is closed")
        length = len(payload)
        header = bytearray()
        header.append(0x80 | (opcode & 0x0F))
        mask_bit = 0x80
        if length < 126:
            header.append(mask_bit | length)
        elif length < 65536:
            header.append(mask_bit | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(mask_bit | 127)
            header.extend(struct.pack("!Q", length))
        mask = os.urandom(4)
        header.extend(mask)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self._sock.sendall(header + masked)
