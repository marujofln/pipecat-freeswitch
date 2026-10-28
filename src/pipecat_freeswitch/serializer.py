#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""FreeSWITCH mod_audio_stream WebSocket protocol serializer for Pipecat."""

import base64
import json

from loguru import logger
from pipecat.audio.resamplers.base_audio_resampler import SoxrQuality
from pipecat.audio.resamplers.soxr_stream_resampler import SOXRStreamAudioResampler
from pipecat.audio.utils import create_stream_resampler, pcm_to_wav
from pipecat.frames.frames import (
    AudioRawFrame,
    CancelFrame,
    EndFrame,
    Frame,
    InputAudioRawFrame,
    InterruptionFrame,
)
from pipecat.processors.frame_processor import FrameProcessorSetup
from pipecat.serializers.base_serializer import FrameSerializer

from pipecat_freeswitch.control import FreeSWITCHControl, is_valid_uuid

# Raw streamAudio payloads become .r8/.r16/... files, and mod_sndfile only plays raw audio at these
# rates. 48 kHz is sent as WAV instead, which mod_sndfile plays at any rate.
_RAW_SAMPLE_RATES = (8000, 16000, 24000, 32000)
_WAV_SAMPLE_RATES = (48000,)
_OUT_SAMPLE_RATES = _RAW_SAMPLE_RATES + _WAV_SAMPLE_RATES


class FreeSWITCHFrameSerializer(FrameSerializer):
    """Serializer for the FreeSWITCH mod_audio_stream WebSocket protocol (open source edition).

    Caller audio arrives as binary L16 frames and becomes ``InputAudioRawFrame``.
    Bot audio leaves as ``streamAudio`` JSON messages, one per output frame, so
    the block size is the transport's ``audio_out_10ms_chunks`` setting.

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
    ``CancelFrame``.

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
            sample_rate: Optional override for the pipeline input sample rate.
            auto_hang_up: Whether to hang up the call on EndFrame or CancelFrame.
            hang_up_timeout_secs: Maximum time to wait on EndFrame for queued
                audio to finish playing before hanging up.
            ignore_rtvi_messages: Inherited from base FrameSerializer, defaults to True.
        """

        freeswitch_sample_rate: int = 8000
        freeswitch_in_sample_rate: int | None = None
        freeswitch_out_sample_rate: int | None = None
        output_resampler_quality: SoxrQuality = "QQ"
        sample_rate: int | None = None
        auto_hang_up: bool = True
        hang_up_timeout_secs: float = 30.0

    def __init__(
        self,
        call_uuid: str | None = None,
        esl_host: str = "127.0.0.1",
        esl_port: int = 8021,
        esl_password: str = "ClueCon",
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

        self._sent_messages = (
            0  # streamAudio messages sent; mod_audio_stream numbers files the same way
        )
        self._held_audio = bytearray()  # bot audio produced before the control was ready
        self._hangup_attempted = False

    @property
    def call_uuid(self) -> str | None:
        """UUID of the FreeSWITCH channel, once known."""
        return self._call_uuid

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
            self._held_audio.clear()
            await self._control.interrupt(self._sent_messages)
            return None
        elif isinstance(frame, AudioRawFrame):
            audio = await self._output_resampler.resample(
                frame.audio, frame.sample_rate, self._out_sample_rate
            )
            if not self._control.started:
                # Nobody would play it yet: keep it and send it with the next block.
                self._held_audio.extend(audio)
                return None
            if self._held_audio:
                audio = bytes(self._held_audio) + audio
                self._held_audio.clear()
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

        # Return None for unhandled frames
        return None

    async def deserialize(self, data: str | bytes) -> Frame | None:
        """Deserializes mod_audio_stream WebSocket data to Pipecat frames.

        Binary messages are caller audio. The first text message is the
        metadata given to ``uuid_audio_stream``: the channel UUID, either as
        plain text or as a JSON object with a ``uuid`` field. Anything that is
        not a canonical UUID is rejected, since the UUID goes into event socket
        commands and any WebSocket client can send this message.

        Args:
            data: The raw WebSocket data from FreeSWITCH.

        Returns:
            An InputAudioRawFrame for caller audio, or None otherwise.
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
            logger.debug(f"{self}: ignoring text message: {data[:200]!r}")
        return None

    async def _end_call(self, frame: EndFrame | CancelFrame):
        if self._params.auto_hang_up and not self._hangup_attempted:
            self._hangup_attempted = True
            if isinstance(frame, EndFrame):
                # The transport sends audio faster than real time, so a lot of it
                # can still be queued in FreeSWITCH: let it finish.
                await self._control.wait_for_playback(
                    self._sent_messages, self._params.hang_up_timeout_secs
                )
            await self._control.hang_up()
        await self._control.stop()

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
