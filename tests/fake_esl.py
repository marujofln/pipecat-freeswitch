#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""A fake FreeSWITCH event socket for unit tests."""

import asyncio
import json
from urllib.parse import quote


class FakeESLServer:
    """Speaks enough of the inbound ESL protocol to test the control client.

    It authenticates any client using ``password``, answers every command with
    ``+OK``, records the commands it received and can emit events to all
    connected clients.
    """

    def __init__(self, password: str = "ClueCon"):
        self.password = password
        self.commands: list[str] = []
        self.port = 0
        self._server: asyncio.base_events.Server | None = None
        self._writers: list[asyncio.StreamWriter] = []
        self._command_seen = asyncio.Event()

    async def start(self):
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self):
        for writer in self._writers:
            writer.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def wait_for_command(self, prefix: str, timeout: float = 2.0) -> str:
        """Wait until a command starting with ``prefix`` has been received."""

        async def wait() -> str:
            while True:
                for command in self.commands:
                    if command.startswith(prefix):
                        return command
                self._command_seen.clear()
                await self._command_seen.wait()

        return await asyncio.wait_for(wait(), timeout)

    async def emit(self, headers: dict[str, str], body: str = ""):
        """Send an event in plain format to every connected client."""
        lines = [f"{key}: {quote(value)}" for key, value in headers.items()]
        if body:
            lines.append(f"Content-Length: {len(body.encode())}")
        event = "\n".join(lines) + "\n\n" + body
        message = (
            f"Content-Type: text/event-plain\nContent-Length: {len(event.encode())}\n\n{event}"
        )
        for writer in self._writers:
            writer.write(message.encode())
            await writer.drain()

    async def emit_play(self, uuid: str, index: int, ext: str = "r8"):
        """Emit mod_audio_stream::play for the file of the ``index``-th streamAudio message."""
        body = json.dumps(
            {"audioDataType": "raw", "sampleRate": 8000, "file": play_path(uuid, index, ext)}
        )
        await self.emit(
            {"Event-Name": "CUSTOM", "Event-Subclass": "mod_audio_stream::play", "Unique-ID": uuid},
            body,
        )

    async def emit_playback_complete(self, uuid: str, index: int, ext: str = "r8"):
        """Emit CHANNEL_EXECUTE_COMPLETE for the playback of one file."""
        await self.emit(
            {
                "Event-Name": "CHANNEL_EXECUTE_COMPLETE",
                "Unique-ID": uuid,
                "Application": "playback",
                "Application-Data": play_path(uuid, index, ext),
            }
        )

    async def emit_hangup(self, uuid: str):
        """Emit CHANNEL_HANGUP_COMPLETE."""
        await self.emit({"Event-Name": "CHANNEL_HANGUP_COMPLETE", "Unique-ID": uuid})

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._writers.append(writer)
        writer.write(b"Content-Type: auth/request\n\n")
        await writer.drain()
        try:
            while True:
                command = await self._read_command(reader)
                if command is None:
                    break
                if command.startswith("auth "):
                    ok = command == f"auth {self.password}"
                    reply = "+OK accepted" if ok else "-ERR invalid"
                    writer.write(f"Content-Type: command/reply\nReply-Text: {reply}\n\n".encode())
                    await writer.drain()
                    if not ok:
                        break
                    continue
                self.commands.append(command)
                self._command_seen.set()
                if command.startswith("api "):
                    body = "+OK\n"
                    header = f"Content-Type: api/response\nContent-Length: {len(body)}\n\n"
                    writer.write((header + body).encode())
                else:
                    writer.write(b"Content-Type: command/reply\nReply-Text: +OK\n\n")
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    @staticmethod
    async def _read_command(reader: asyncio.StreamReader) -> str | None:
        lines = []
        while True:
            line = await reader.readline()
            if not line:
                return None
            line = line.decode().rstrip("\n")
            if not line:
                if lines:
                    return "\n".join(lines)
                continue
            lines.append(line)


def play_path(uuid: str, index: int, ext: str = "r8") -> str:
    """Path mod_audio_stream uses for the file of the ``index``-th streamAudio message."""
    return f"/tmp/{uuid}_{index}.tmp.{ext}"
