#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""FreeSWITCH mod_audio_stream WebSocket protocol serializer for Pipecat."""

import base64
import json

from loguru import logger
from pipecat.audio.dtmf.types import KeypadEntry
from pipecat.audio.resamplers.base_audio_resampler import SoxrQuality
from pipecat.audio.resamplers.soxr_stream_resampler import SOXRStreamAudioResampler
from pipecat.audio.utils import create_stream_resampler, pcm_to_wav, scale_audio
from pipecat.frames.frames import (
    AudioRawFrame,
    CancelFrame,
    EndFrame,
    Frame,
    InputAudioRawFrame,
    InputDTMFFrame,
    InterruptionFrame,
)
from pipecat.processors.frame_processor import FrameProcessorSetup
from pipecat.serializers.base_serializer import FrameSerializer

from pipecat_freeswitch.control import DEFAULT_ESL_PASSWORD, FreeSWITCHControl, is_valid_uuid
from pipecat_freeswitch.lookahead import FreeSWITCHLookahead

# Raw streamAudio payloads become .r8/.r16/... files, and mod_sndfile only plays raw audio at these
# rates. 48 kHz is sent as WAV instead, which mod_sndfile plays at any rate.
_RAW_SAMPLE_RATES = (8000, 16000, 24000, 32000)
_WAV_SAMPLE_RATES = (48000,)
_OUT_SAMPLE_RATES = _RAW_SAMPLE_RATES + _WAV_SAMPLE_RATES

# Bot audio kept while the channel UUID is unknown. The UUID is mod_audio_stream's first message, so
# hitting this means it never came: keep the latest audio instead of growing without bound.
_MAX_HELD_SECS = 5

# Longest block sent in one streamAudio message when the audio comes from the lookahead processor,
# which can hand over a whole utterance at once. The rest goes out with the next chunks the
# transport writes.
_MAX_BLOCK_SECS = 5


class FreeSWITCHFrameSerializer(FrameSerializer):
    """Serializer for the FreeSWITCH mod_audio_stream WebSocket protocol (open source edition).

    Caller audio arrives as binary L16 frames and becomes ``InputAudioRawFrame``.
    Bot audio leaves as ``streamAudio`` JSON messages, one per output frame, so
    the block size is the transport's ``audio_out_10ms_chunks`` setting. The
    transport sends those frames at real time. With the processor returned by
    :meth:`lookahead` in the pipeline, the serializer sends the audio as soon
    as it is produced instead, in blocks that grow while the bot speaks.

    Bot audio should reach FreeSWITCH at the channel's rate (8000 for G.711,
    16000 for G.722, 48000 for Opus): FreeSWITCH resamples each played file on
    its own, which adds artefacts at every block seam. Bot audio is therefore
    sent at ``freeswitch_out_sample_rate`` (by default ``freeswitch_sample_rate``,
    8000), resampled by the serializer if needed. The simplest setup also runs
    the pipeline output at that rate (``PipelineParams.audio_out_sample_rate``),
    so nothing is resampled at all.

    mod_audio_stream only writes each ``streamAudio`` block to a file and raises
    an event; it does not play it. The serializer therefore drives the channel
    through the FreeSWITCH event socket with :class:`FreeSWITCHControl`: it
    queues each file for playback, stops playback on interruption
    (``uuid_break all``) and, when auto_hang_up is enabled, hangs up on
    ``EndFrame`` once the queued audio has finished playing, or right away on
    ``CancelFrame``. DTMF digits pressed by the caller are forwarded by the
    control to the WebSocket and become ``InputDTMFFrame``.

    The channel UUID is the last argument of ``uuid_audio_stream ... start``
    and arrives as the first text message. Pass it as ``call_uuid`` when the
    application reads that message before building the pipeline; otherwise the
    serializer takes it from the first message and holds bot audio until the
    event socket is ready.
    """

    class InputParams(FrameSerializer.InputParams):
        """Configuration parameters for FreeSWITCHFrameSerializer.

        Parameters:
            freeswitch_sample_rate: Default sample rate for both directions.
                Defaults to 8000.
            freeswitch_in_sample_rate: Rate of the caller audio, as given to
                ``uuid_audio_stream`` (``8k`` → 8000, ``16k`` → 16000). Defaults
                to ``freeswitch_sample_rate``.
            freeswitch_out_sample_rate: Rate of the bot audio sent in
                ``streamAudio``: 8000, 16000, 24000, 32000 (raw) or 48000 (WAV).
                Should match the channel rate. Defaults to
                ``freeswitch_sample_rate``. Bot audio at another rate is
                resampled by the serializer.
            output_resampler_quality: SOXR quality used when bot audio must be
                resampled. Defaults to "QQ", the only preset that holds back no
                audio: higher presets keep up to ~110 ms inside the resampler,
                and the end of each utterance would be lost because nothing
                flushes it.
            output_gain_db: Gain applied to bot audio before it is sent, in dB
                (negative to attenuate), with samples clipped to 16 bits.
                Defaults to 0 (unchanged). TTS services often deliver speech
                far louder than telephone level (about -26 dBFS active speech,
                ITU-T P.56): too loud, it distorts on phones and makes their
                echo suppression gate the caller's microphone, which defeats
                barge-in.
            sample_rate: Optional override for the pipeline input sample rate.
            auto_hang_up: Whether to hang up the call on EndFrame or CancelFrame.
            hang_up_timeout_secs: Maximum time to wait on EndFrame for queued
                audio to finish playing before hanging up.
            lookahead_margin_secs: With :meth:`lookahead` in the pipeline, how
                far ahead of the transport's clock the audio already sent must
                be, or the next block is sent. The transport itself runs one
                chunk ahead of real time, so this is how much audio FreeSWITCH
                still has to play, beyond that chunk, when the next block
                leaves. A larger margin protects against blocks that arrive
                late (a gap in the bot's voice) and makes the first blocks
                smaller. At least one chunk is always used.
            ignore_rtvi_messages: Inherited from base FrameSerializer, defaults to True.
        """

        freeswitch_sample_rate: int = 8000
        freeswitch_in_sample_rate: int | None = None
        freeswitch_out_sample_rate: int | None = None
        output_resampler_quality: SoxrQuality = "QQ"
        output_gain_db: float = 0.0
        sample_rate: int | None = None
        auto_hang_up: bool = True
        hang_up_timeout_secs: float = 30.0
        lookahead_margin_secs: float = 0.1

    def __init__(
        self,
        call_uuid: str | None = None,
        esl_host: str = "127.0.0.1",
        esl_port: int = 8021,
        esl_password: str = DEFAULT_ESL_PASSWORD,
        params: InputParams | None = None,
        *,
        control: FreeSWITCHControl | None = None,
    ):
        """Initialize the FreeSWITCHFrameSerializer.

        Args:
            call_uuid: UUID of the FreeSWITCH channel. Optional: if omitted, it
                is taken from the first text message mod_audio_stream sends.
            esl_host: FreeSWITCH event socket host.
            esl_port: FreeSWITCH event socket port.
            esl_password: FreeSWITCH event socket password.
            params: Configuration parameters.
            control: Control client to use instead of one built from the
                ``esl_*`` arguments.

        Raises:
            ValueError: If a sample rate is not supported by mod_audio_stream,
                or ``call_uuid`` is not a canonical UUID.
        """
        params = params or FreeSWITCHFrameSerializer.InputParams()
        super().__init__(params)
        self._params: FreeSWITCHFrameSerializer.InputParams = params

        self._in_sample_rate = (
            self._params.freeswitch_in_sample_rate or self._params.freeswitch_sample_rate
        )
        self._out_sample_rate = (
            self._params.freeswitch_out_sample_rate or self._params.freeswitch_sample_rate
        )
        if self._in_sample_rate <= 0 or self._in_sample_rate % 8000:
            raise ValueError(
                f"freeswitch_in_sample_rate must be a multiple of 8000, got {self._in_sample_rate}"
            )
        if self._out_sample_rate not in _OUT_SAMPLE_RATES:
            raise ValueError(
                f"freeswitch_out_sample_rate must be one of {_OUT_SAMPLE_RATES}, "
                f"got {self._out_sample_rate}"
            )
        if call_uuid is not None and not is_valid_uuid(call_uuid):
            raise ValueError(f"call_uuid must be a canonical UUID, got {call_uuid!r}")

        self._call_uuid = call_uuid
        self._control = control or FreeSWITCHControl(esl_host, esl_port, esl_password)

        self._sample_rate = 0  # Pipeline input rate

        self._input_resampler = create_stream_resampler(
            clear_after_secs=self._params.resampler_clear_after_secs
        )
        self._output_resampler = SOXRStreamAudioResampler(
            quality=self._params.output_resampler_quality,
            clear_after_secs=self._params.resampler_clear_after_secs,
        )
        self._output_gain = 10 ** (self._params.output_gain_db / 20)

        self._sent_messages = (
            0  # streamAudio messages sent; mod_audio_stream numbers files the same way
        )
        # Bot audio at the output rate that is still to be sent: produced before the control was
        # ready or, with lookahead, ahead of the transport's clock.
        self._buffer = bytearray()
        self._warned_held_audio = False
        self._hangup_attempted = False

        self._lookahead: FreeSWITCHLookahead | None = None
        # Whether the lookahead processor is running in the pipeline. Only then does the audio
        # come from it, and the transport's audio frames only tell the time.
        self._lookahead_active = False
        # Interruptions the lookahead processor has seen and the transport has not written yet.
        self._interruptions_ahead = 0
        self._lookahead_got_audio = False
        self._warned_lookahead_idle = False
        # Bytes of audio sent beyond what the transport has written, at the output rate.
        self._ahead = 0
        self._lookahead_margin = 2 * int(
            max(0.0, self._params.lookahead_margin_secs) * self._out_sample_rate
        )

    @property
    def call_uuid(self) -> str | None:
        """UUID of the FreeSWITCH channel, once known."""
        return self._call_uuid

    def lookahead(self) -> FreeSWITCHLookahead:
        """Processor that lets the serializer send bot audio ahead of the transport's clock.

        Place it right before ``transport.output()``, after every processor
        that produces or changes bot audio. The serializer then takes the bot
        audio from it, as soon as it is produced, and the chunks the transport
        writes only tell the time: whenever the audio already sent is less
        than ``lookahead_margin_secs`` ahead of that clock, the serializer
        sends everything it holds as one block. The first block is as short as
        the transport's chunk, and each one after it holds what was produced
        while the one before played, so FreeSWITCH plays the speech in a few
        growing playbacks instead of one per chunk. See
        :class:`FreeSWITCHLookahead` for what else it changes. Without it in
        the pipeline, the serializer sends the transport's chunks as they
        come.

        Returns:
            The lookahead processor of this serializer, always the same one.
        """
        if self._lookahead is None:
            self._lookahead = FreeSWITCHLookahead(self)
        return self._lookahead

    async def setup(self, setup: FrameProcessorSetup):
        """Sets up the serializer with pipeline configuration.

        Connects to the event socket if the channel UUID is already known.

        Args:
            setup: Configuration object containing setup parameters.
        """
        self._sample_rate = self._params.sample_rate or setup.audio_in_sample_rate
        if setup.audio_out_sample_rate != self._out_sample_rate:
            logger.debug(
                f"{self}: resampling bot audio from {setup.audio_out_sample_rate} "
                f"to {self._out_sample_rate}"
            )
        if self._call_uuid and not self._control.started:
            await self._control.start(self._call_uuid)

    async def serialize(self, frame: Frame) -> str | bytes | None:
        """Serializes a Pipecat frame to the mod_audio_stream protocol.

        Audio frames become ``streamAudio`` messages. Interruptions, EndFrame
        and CancelFrame are handled through the event socket and produce no
        message.

        Args:
            frame: The Pipecat frame to serialize.

        Returns:
            Serialized data as a string, or None if there is nothing to send.
        """
        if isinstance(frame, (EndFrame, CancelFrame)):
            await self._end_call(frame)
            return None
        elif isinstance(frame, InterruptionFrame):
            if self._interruptions_ahead:
                # The lookahead processor already dropped the audio from before this
                # interruption; what the buffer holds now came after it.
                self._interruptions_ahead -= 1
            else:
                self._buffer.clear()
            self._ahead = 0
            await self._control.interrupt(self._sent_messages)
            return None
        elif isinstance(frame, AudioRawFrame):
            if not self._lookahead_active:
                await self._buffer_audio(frame)
                return self._next_block()
            # The audio already came from the lookahead processor: this frame only tells that
            # the transport's clock moved on. Until the transport writes an interruption the
            # processor has seen, its frames are from before it and trigger nothing.
            if self._interruptions_ahead:
                return None
            if (
                not self._lookahead_got_audio
                and not self._warned_lookahead_idle
                and frame.audio.strip(b"\x00")
            ):
                self._warned_lookahead_idle = True
                logger.warning(
                    f"{self}: the transport is writing audio the lookahead processor never "
                    "saw, so it will not be played: place the processor right before "
                    "transport.output()"
                )
            chunk_bytes = len(frame.audio) * self._out_sample_rate // frame.sample_rate
            self._ahead -= chunk_bytes
            block = None
            # With a margin of at least one chunk, the last chunk of an utterance always
            # sends what is left of it.
            if self._ahead < max(self._lookahead_margin, chunk_bytes):
                held = len(self._buffer)
                block = self._next_block(
                    max(_MAX_BLOCK_SECS * self._out_sample_rate * 2, chunk_bytes)
                )
                self._ahead += held - len(self._buffer)
            if not self._buffer:
                # Everything was sent, so what the transport wrote beyond it is audio it made
                # itself (padding, silence), which is not played and takes no time.
                self._ahead = max(self._ahead, 0)
            return block

        # Return None for unhandled frames
        return None

    async def deserialize(self, data: str | bytes) -> Frame | None:
        """Deserializes mod_audio_stream WebSocket data to Pipecat frames.

        Binary messages are caller audio. The first text message is the
        metadata given to ``uuid_audio_stream``: the channel UUID, either as
        plain text or as a JSON object with a ``uuid`` field. Anything that is
        not a canonical UUID is rejected, since the UUID goes into event socket
        commands and any WebSocket client can send this message. Later text
        messages are DTMF digits forwarded by the control,
        ``{"event":"dtmf","digit":"5"}``.

        Args:
            data: The raw WebSocket data from FreeSWITCH.

        Returns:
            An InputAudioRawFrame for caller audio, an InputDTMFFrame for a
            DTMF digit, or None otherwise.
        """
        if isinstance(data, bytes):
            audio = await self._input_resampler.resample(
                data, self._in_sample_rate, self._sample_rate
            )
            if not audio:
                return None
            return InputAudioRawFrame(audio=audio, num_channels=1, sample_rate=self._sample_rate)

        if self._call_uuid is None:
            self._call_uuid = self._parse_uuid(data)
            if self._call_uuid:
                await self._control.start(self._call_uuid)
            else:
                logger.error(f"{self}: no channel UUID in the first message: {data[:200]!r}")
        else:
            return self._parse_dtmf(data)
        return None

    async def cleanup(self):
        """Hang up if still needed and close the event socket connection.

        Call this from the transport's ``on_client_disconnected`` handler. Once
        the WebSocket is closed, the transport no longer passes frames to the
        serializer, so the ``CancelFrame`` that ends the pipeline never reaches
        it: without this call, a WebSocket that drops while the call is up
        would leave the call parked and the event socket connection open.
        Safe to call more than once, and after ``EndFrame`` or ``CancelFrame``.
        """
        await super().cleanup()
        await self._end_call(CancelFrame())

    def _lookahead_started(self):
        """The lookahead processor is running: bot audio comes from it from now on."""
        self._lookahead_active = True

    async def _lookahead_audio(self, frame: AudioRawFrame):
        """Take bot audio from the lookahead processor, ahead of the transport's clock."""
        self._lookahead_got_audio = True
        await self._buffer_audio(frame)

    def _lookahead_interrupted(self):
        """An interruption passed the lookahead processor: drop the audio from before it."""
        self._buffer.clear()
        self._interruptions_ahead += 1

    def _lookahead_bot_stopped(self):
        """The transport finished an utterance, dropping whatever it still held of it.

        Audio it dropped was sent but will never be written, so the count of audio sent ahead
        would stay too high and delay the next blocks. Starting over only sends them sooner.
        """
        self._ahead = 0

    async def _buffer_audio(self, frame: AudioRawFrame):
        """Add bot audio to the buffer, at the output rate and with the gain applied."""
        audio = await self._output_resampler.resample(
            frame.audio, frame.sample_rate, self._out_sample_rate
        )
        if audio and self._output_gain != 1.0:
            audio = scale_audio(audio, self._output_gain)
        self._buffer.extend(audio)
        if self._control.started:
            return
        # Nobody would play it yet: keep it, up to a limit, and send it with the next block.
        excess = len(self._buffer) - _MAX_HELD_SECS * self._out_sample_rate * 2
        if excess > 0:
            del self._buffer[: excess + excess % 2]
            if not self._warned_held_audio:
                self._warned_held_audio = True
                logger.warning(
                    f"{self}: no channel UUID yet, keeping only the last "
                    f"{_MAX_HELD_SECS}s of bot audio"
                )

    def _next_block(self, max_bytes: int | None = None) -> str | None:
        """Take the buffered audio, or its first ``max_bytes``, as a ``streamAudio`` message."""
        if not self._control.started or not self._buffer:
            return None
        size = len(self._buffer) if max_bytes is None else min(len(self._buffer), max_bytes)
        size -= size % 2
        audio = bytes(self._buffer[:size])
        del self._buffer[:size]
        if not audio:
            return None

        self._sent_messages += 1
        if self._out_sample_rate in _WAV_SAMPLE_RATES:
            audio_type = "wav"
            audio = pcm_to_wav(audio, self._out_sample_rate, 1)
        else:
            audio_type = "raw"
        return json.dumps(
            {
                "type": "streamAudio",
                "data": {
                    "audioDataType": audio_type,
                    "sampleRate": self._out_sample_rate,
                    "audioData": base64.b64encode(audio).decode("utf-8"),
                },
            }
        )

    async def _end_call(self, frame: EndFrame | CancelFrame):
        if self._params.auto_hang_up and not self._hangup_attempted:
            self._hangup_attempted = True
            if isinstance(frame, EndFrame):
                # The last blocks are still playing, or on their way to FreeSWITCH: let
                # them finish.
                await self._control.wait_for_playback(
                    self._sent_messages, self._params.hang_up_timeout_secs
                )
            await self._control.hang_up()
        await self._control.stop()

    def _parse_dtmf(self, text: str) -> InputDTMFFrame | None:
        try:
            message = json.loads(text)
        except json.JSONDecodeError:
            message = None
        if not isinstance(message, dict) or message.get("event") != "dtmf":
            logger.debug(f"{self}: ignoring text message: {text[:200]!r}")
            return None
        digit = message.get("digit")
        try:
            return InputDTMFFrame(KeypadEntry(digit))
        except ValueError:
            # A-D exist in RFC 4733 but not on phone keypads, nor in KeypadEntry.
            logger.debug(f"{self}: ignoring DTMF digit {digit!r}")
            return None

    @staticmethod
    def _parse_uuid(text: str) -> str | None:
        text = text.strip()
        if text.startswith("{"):
            try:
                value = json.loads(text).get("uuid")
            except (json.JSONDecodeError, AttributeError):
                return None
        else:
            value = text
        return value if isinstance(value, str) and is_valid_uuid(value) else None
