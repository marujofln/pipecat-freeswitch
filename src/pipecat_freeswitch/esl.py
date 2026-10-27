#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Minimal asyncio client for the FreeSWITCH inbound event socket (ESL)."""

import asyncio
from urllib.parse import unquote


class ESLError(Exception):
    """Raised when the event socket refuses a command or the connection is lost."""


class ESLConnection:
    """One authenticated inbound connection to the FreeSWITCH event socket.

    Command replies and events arrive on the same socket. A background task reads
    the socket and routes events to :meth:`next_event` and replies to the
    command that is waiting for them. Commands are serialized with a lock, so
    replies always match the command that produced them.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Initialize the connection from an open stream pair.

        Use :meth:`connect` instead of calling this directly.

        Args:
            reader: Stream reader of the TCP connection.
            writer: Stream writer of the TCP connection.
        """
        self._reader = reader
        self._writer = writer
        self._replies: asyncio.Queue[tuple[dict[str, str], str] | None] = asyncio.Queue()
        self._events: asyncio.Queue[dict[str, str] | None] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._reader_task: asyncio.Task | None = None
        self._closed = False

    @classmethod
    async def connect(
        cls, host: str, port: int, password: str, timeout: float = 5.0
    ) -> "ESLConnection":
        """Open the socket, authenticate and start reading.

        Args:
            host: Event socket host.
            port: Event socket port (8021 by default in FreeSWITCH).
            password: Event socket password.
            timeout: Seconds to wait for the connection and authentication.

        Returns:
            An authenticated connection.

        Raises:
            ESLError: If authentication fails.
            OSError: If the socket cannot be opened.
        """
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
        conn = cls(reader, writer)
        try:
            headers, _ = await asyncio.wait_for(conn._read_message(), timeout)
            if headers.get("Content-Type") != "auth/request":
                raise ESLError(f"unexpected greeting from event socket: {headers}")
            writer.write(f"auth {password}\n\n".encode())
            await writer.drain()
            headers, _ = await asyncio.wait_for(conn._read_message(), timeout)
            if not headers.get("Reply-Text", "").startswith("+OK"):
                raise ESLError(f"event socket authentication failed: {headers.get('Reply-Text')}")
        except BaseException:
            writer.close()
            raise
        conn._reader_task = asyncio.create_task(conn._read_loop())
        return conn

    @property
    def closed(self) -> bool:
        """Whether the connection has been closed or lost."""
        return self._closed

    async def close(self):
        """Close the connection."""
        if self._reader_task:
            self._reader_task.cancel()
        if not self._writer.is_closing():
            self._writer.close()
        self._closed = True

    async def send(self, command: str) -> tuple[dict[str, str], str]:
        """Send one command and wait for its reply.

        Args:
            command: The command, without the terminating blank line.

        Returns:
            The reply headers and body.

        Raises:
            ESLError: If the connection is closed.
        """
        async with self._lock:
            if self._closed:
                raise ESLError("event socket connection is closed")
            self._writer.write(f"{command}\n\n".encode())
            await self._writer.drain()
            reply = await self._replies.get()
            if reply is None:
                await self._replies.put(None)
                raise ESLError("event socket connection closed")
            return reply

    async def api(self, command: str) -> str:
        """Run a blocking ``api`` command.

        Args:
            command: The API command and its arguments, e.g. ``uuid_kill <uuid>``.

        Returns:
            The command output.
        """
        _, body = await self.send(f"api {command}")
        return body.strip()

    async def execute(self, uuid: str, app: str, arg: str) -> str:
        """Queue a dialplan application on a channel with ``sendmsg``.

        Unlike ``uuid_broadcast``, this sends no ``lead-frames`` header, so
        consecutive applications run back to back with no discarded frames.

        Args:
            uuid: Channel UUID.
            app: Application name, e.g. ``playback``.
            arg: Application argument.

        Returns:
            The reply text (``+OK`` on success).
        """
        headers, _ = await self.send(
            f"sendmsg {uuid}\ncall-command: execute\nexecute-app-name: {app}\n"
            f"execute-app-arg: {arg}\nevent-lock: true"
        )
        return headers.get("Reply-Text", "")

    async def subscribe(self, events: str):
        """Subscribe to events in plain format.

        Args:
            events: Space-separated event names; custom subclasses go after ``CUSTOM``.
        """
        await self.send(f"event plain {events}")

    async def filter(self, header: str, value: str):
        """Only deliver events whose ``header`` equals ``value``.

        Args:
            header: Event header name, e.g. ``Unique-ID``.
            value: Value to match.
        """
        await self.send(f"filter {header} {value}")

    async def next_event(self) -> dict[str, str]:
        """Wait for the next subscribed event.

        Returns:
            The event headers (URL-decoded); the event body, if any, is under ``_body``.

        Raises:
            ESLError: Once the connection is closed.
        """
        event = await self._events.get()
        if event is None:
            await self._events.put(None)
            raise ESLError("event socket connection closed")
        return event

    async def _read_message(self) -> tuple[dict[str, str], str]:
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

    async def _read_loop(self):
        try:
            while True:
                headers, body = await self._read_message()
                content_type = headers.get("Content-Type")
                if content_type == "text/event-plain":
                    await self._events.put(parse_plain_event(body))
                elif content_type in ("command/reply", "api/response"):
                    await self._replies.put((headers, body))
                elif content_type == "text/disconnect-notice":
                    break
        except (OSError, asyncio.IncompleteReadError):
            pass
        finally:
            self._closed = True
            await self._events.put(None)
            await self._replies.put(None)


def parse_plain_event(body: str) -> dict[str, str]:
    """Parse a ``text/event-plain`` body.

    Args:
        body: The body of an ``event plain`` message.

    Returns:
        The event headers, URL-decoded; the event's own body, if any, under ``_body``.
    """
    head, _, rest = body.partition("\n\n")
    event: dict[str, str] = {}
    for line in head.splitlines():
        key, sep, value = line.partition(": ")
        if sep:
            event[key] = unquote(value)
    if "Content-Length" in event:
        event["_body"] = rest[: int(event["Content-Length"])]
    return event
