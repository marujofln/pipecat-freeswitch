"""Minimal asyncio client for the FreeSWITCH inbound event socket (ESL).

Standard library only, so the experiment runner works without installing anything.
"""

import asyncio
from urllib.parse import unquote


class EslConnection:
    """One authenticated inbound ESL connection.

    Replies to commands and events arrive on the same socket. Events go to ``events``,
    and command replies go to ``_replies`` in the order they arrive.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self._replies: asyncio.Queue[tuple[dict, str]] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self.events: asyncio.Queue[dict] = asyncio.Queue()
        self._reader_task: asyncio.Task | None = None

    @classmethod
    async def connect(cls, host: str, port: int, password: str) -> "EslConnection":
        """Open the socket, authenticate and start the background reader."""
        reader, writer = await asyncio.open_connection(host, port)
        conn = cls(reader, writer)
        headers, _ = await conn._read_message()  # Content-Type: auth/request
        writer.write(f"auth {password}\n\n".encode())
        await writer.drain()
        headers, _ = await conn._read_message()
        if not headers.get("Reply-Text", "").startswith("+OK"):
            raise ConnectionError(f"ESL auth failed: {headers}")
        conn._reader_task = asyncio.create_task(conn._read_loop())
        return conn

    async def close(self) -> None:
        """Stop the reader and close the socket."""
        if self._reader_task:
            self._reader_task.cancel()
        self._writer.close()

    async def send(self, command: str) -> tuple[dict, str]:
        """Send one command and return its (headers, body) reply."""
        async with self._lock:
            self._writer.write(f"{command}\n\n".encode())
            await self._writer.drain()
            return await self._replies.get()

    async def api(self, command: str) -> str:
        """Run a blocking ``api`` command and return its body."""
        _, body = await self.send(f"api {command}")
        return body.strip()

    async def execute(self, uuid: str, app: str, arg: str) -> str:
        """Queue a dialplan app on a channel via ``sendmsg``, like ``uuid_broadcast`` but without
        its hardcoded ``lead-frames: 5`` (100 ms of discarded frames at 20 ms ptime per app).
        """
        headers, _ = await self.send(
            f"sendmsg {uuid}\ncall-command: execute\nexecute-app-name: {app}\n"
            f"execute-app-arg: {arg}\nevent-lock: true"
        )
        return headers.get("Reply-Text", "")

    async def subscribe(self, events: str) -> None:
        """Subscribe to events, e.g. ``CHANNEL_HANGUP CUSTOM mod_audio_stream::play``."""
        await self.send(f"event plain {events}")

    async def _read_message(self) -> tuple[dict, str]:
        headers: dict[str, str] = {}
        while True:
            line = (await self._reader.readline()).decode()
            if not line:
                raise asyncio.IncompleteReadError(b"", None)
            line = line.rstrip("\n")
            if not line:
                if headers:
                    break
                continue
            key, _, value = line.partition(": ")
            headers[key] = value
        body = ""
        if "Content-Length" in headers:
            body = (await self._reader.readexactly(int(headers["Content-Length"]))).decode()
        return headers, body

    async def _read_loop(self) -> None:
        while True:
            headers, body = await self._read_message()
            content_type = headers.get("Content-Type")
            if content_type == "text/event-plain":
                await self.events.put(parse_plain_event(body))
            elif content_type in ("command/reply", "api/response"):
                await self._replies.put((headers, body))


def parse_plain_event(body: str) -> dict:
    """Parse a ``text/event-plain`` body into a dict; the event body, if any, goes in ``_body``."""
    head, _, rest = body.partition("\n\n")
    event = {}
    for line in head.splitlines():
        key, sep, value = line.partition(": ")
        if sep:
            event[key] = unquote(value)
    if "Content-Length" in event:
        event["_body"] = rest[: int(event["Content-Length"])]
    return event


def event_time(event: dict) -> float:
    """Return the event's FreeSWITCH timestamp in epoch seconds."""
    return int(event["Event-Date-Timestamp"]) / 1_000_000
