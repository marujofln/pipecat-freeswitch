#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Call control over the FreeSWITCH event socket: playback, barge-in and hang-up."""

import asyncio
import json
import re

from loguru import logger

from pipecat_freeswitch.esl import ESLConnection, ESLError

# mod_audio_stream names the file of the n-th streamAudio message <temp>/<uuid>_<n>.tmp.<ext>.
_FILE_INDEX = re.compile(r"_(\d+)\.tmp\.[^./]+$")

_EVENTS = "CHANNEL_EXECUTE_COMPLETE CHANNEL_HANGUP_COMPLETE CUSTOM mod_audio_stream::play"

# The channel UUID goes into event socket commands, where a newline would start another command.
_UUID = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")


def is_valid_uuid(value: str) -> bool:
    """Whether ``value`` is a channel UUID in canonical form (8-4-4-4-12 hex digits).

    Args:
        value: The candidate UUID.

    Returns:
        True if it is safe to use in event socket commands.
    """
    return _UUID.fullmatch(value) is not None


class FreeSWITCHControl:
    """Drives one FreeSWITCH channel for a mod_audio_stream session.

    The open source mod_audio_stream does not play the audio it receives: it
    writes each ``streamAudio`` message to a file and raises
    ``mod_audio_stream::play``. This class listens for that event on the
    channel and queues ``playback`` of each file with ESL ``sendmsg``, which
    (unlike ``uuid_broadcast``) adds no silence between files. It also
    interrupts playback and hangs up the call.

    Files are numbered by mod_audio_stream in the order the messages were
    sent. After an interruption, files from messages sent before it are
    dropped, so audio still in flight when the user barged in is never played.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8021,
        password: str = "ClueCon",
        *,
        connect_timeout_secs: float = 5.0,
    ):
        """Initialize the control client.

        Args:
            host: FreeSWITCH event socket host.
            port: FreeSWITCH event socket port.
            password: FreeSWITCH event socket password.
            connect_timeout_secs: Timeout for connecting and authenticating.
        """
        self._host = host
        self._port = port
        self._password = password
        self._connect_timeout_secs = connect_timeout_secs

        self._conn: ESLConnection | None = None
        self._event_task: asyncio.Task | None = None
        self._uuid: str | None = None

        # Files from messages numbered below this were sent before the last interruption.
        self._discard_below = 0
        # Highest file index announced so far (played or dropped).
        self._last_index = -1
        # Files queued for playback that have not finished yet.
        self._pending: set[str] = set()
        self._warned_file_name = False
        self._channel_gone = False
        self._changed = asyncio.Condition()
        # The input and output transports both set up the serializer, possibly at the same time.
        self._start_lock = asyncio.Lock()

    @property
    def uuid(self) -> str | None:
        """UUID of the controlled channel, once started."""
        return self._uuid

    @property
    def started(self) -> bool:
        """Whether the control is connected and following a channel."""
        return self._conn is not None

    async def start(self, uuid: str):
        """Connect to the event socket and start following a channel.

        Args:
            uuid: UUID of the channel running ``uuid_audio_stream``.

        Raises:
            ValueError: If ``uuid`` is not a canonical UUID.
            ESLError: If the event socket refuses the connection or a command.
            OSError: If the event socket cannot be reached.
        """
        if not is_valid_uuid(uuid):
            raise ValueError(f"not a channel UUID: {uuid!r}")
        async with self._start_lock:
            if self._conn:
                return
            self._uuid = uuid
            conn = await ESLConnection.connect(
                self._host, self._port, self._password, timeout=self._connect_timeout_secs
            )
            await conn.filter("Unique-ID", uuid)
            await conn.subscribe(_EVENTS)
            self._conn = conn
            self._event_task = asyncio.create_task(self._event_loop())
            logger.debug(f"{self}: following channel {uuid}")

    async def stop(self):
        """Stop following the channel and close the connection."""
        if self._event_task:
            self._event_task.cancel()
            self._event_task = None
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def interrupt(self, sent_messages: int):
        """Stop playback and drop audio sent before the interruption.

        Args:
            sent_messages: Number of ``streamAudio`` messages sent so far. Files
                from these messages are dropped even if their ``play`` event
                arrives after this call.
        """
        if not self._conn or self._channel_gone:
            return
        self._discard_below = sent_messages
        try:
            await self._conn.api(f"uuid_break {self._uuid} all")
        except ESLError as e:
            logger.error(f"{self}: uuid_break failed: {e}")
        # `uuid_break all` also removes queued playbacks, which then never complete.
        async with self._changed:
            self._pending.clear()
            self._changed.notify_all()

    async def wait_for_playback(self, sent_messages: int, timeout: float) -> bool:
        """Wait until the audio of every message sent so far has finished playing.

        Args:
            sent_messages: Number of ``streamAudio`` messages sent so far.
            timeout: Maximum time to wait, in seconds.

        Returns:
            True if playback finished (or the channel is gone), False on timeout.
        """
        if not self._conn:
            return True

        def done() -> bool:
            return self._channel_gone or (
                self._last_index >= sent_messages - 1 and not self._pending
            )

        async with self._changed:
            try:
                await asyncio.wait_for(self._changed.wait_for(done), timeout)
                return True
            except TimeoutError:
                logger.warning(
                    f"{self}: playback still pending after {timeout}s "
                    f"({len(self._pending)} files queued), hanging up anyway"
                )
                return False

    async def hang_up(self):
        """Hang up the channel with ``uuid_kill``."""
        if not self._conn or self._channel_gone:
            return
        try:
            result = await self._conn.api(f"uuid_kill {self._uuid}")
            if result.startswith("+OK"):
                logger.info(f"{self}: hung up channel {self._uuid}")
            else:
                logger.debug(f"{self}: uuid_kill {self._uuid}: {result}")
        except ESLError as e:
            logger.error(f"{self}: uuid_kill failed: {e}")

    async def _event_loop(self):
        conn = self._conn
        assert conn is not None
        try:
            while True:
                event = await conn.next_event()
                name = event.get("Event-Name")
                if event.get("Event-Subclass") == "mod_audio_stream::play":
                    await self._handle_play(event)
                elif name == "CHANNEL_EXECUTE_COMPLETE" and event.get("Application") == "playback":
                    async with self._changed:
                        self._pending.discard(event.get("Application-Data", ""))
                        self._changed.notify_all()
                elif name == "CHANNEL_HANGUP_COMPLETE":
                    async with self._changed:
                        self._channel_gone = True
                        self._changed.notify_all()
        except ESLError:
            if not self._channel_gone:
                logger.error(f"{self}: lost the event socket connection; audio will not play")
            async with self._changed:
                self._channel_gone = True
                self._changed.notify_all()

    async def _handle_play(self, event: dict[str, str]):
        try:
            path = json.loads(event.get("_body", ""))["file"]
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning(f"{self}: mod_audio_stream::play event without a file: {event}")
            return
        match = _FILE_INDEX.search(path)
        if match:
            index = int(match.group(1))
        else:
            # Files are announced in send order, so the next index is the one this file has.
            # Without it, the hang-up would wait for this file until it timed out.
            index = self._last_index + 1
            if not self._warned_file_name:
                self._warned_file_name = True
                logger.warning(
                    f"{self}: unexpected mod_audio_stream file name {path}, "
                    "numbering files in arrival order"
                )

        if index < self._discard_below:
            logger.trace(f"{self}: dropping {path}, sent before the last interruption")
        else:
            assert self._conn is not None
            async with self._changed:
                self._pending.add(path)
            result = await self._conn.execute(self._uuid or "", "playback", path)
            if not result.startswith("+OK"):
                logger.error(f"{self}: could not queue playback of {path}: {result}")
                async with self._changed:
                    self._pending.discard(path)

        async with self._changed:
            self._last_index = max(self._last_index, index)
            self._changed.notify_all()

    def __str__(self) -> str:
        """Return a short name for logs."""
        return "FreeSWITCHControl"
