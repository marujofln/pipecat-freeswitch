#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Pipeline processor that lets the serializer send bot audio ahead of the transport's clock."""

from typing import TYPE_CHECKING

from loguru import logger
from pipecat.frames.frames import (
    BotStoppedSpeakingFrame,
    Frame,
    InterruptionFrame,
    OutputAudioRawFrame,
    OutputDTMFFrame,
    OutputDTMFUrgentFrame,
    StartFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

if TYPE_CHECKING:
    from pipecat_freeswitch.serializer import FreeSWITCHFrameSerializer


class FreeSWITCHLookahead(FrameProcessor):
    """Shows bot audio to the serializer before the transport paces it.

    Pipecat's WebSocket transport sends audio at real time, one chunk per chunk
    duration, so FreeSWITCH never holds more than a chunk of audio and has to
    start one playback per chunk. Speech synthesis is faster than that. This
    processor sits right before ``transport.output()`` and hands every bot
    audio frame to the serializer as soon as it is produced. The serializer
    then sends the audio it holds in blocks that grow while the bot speaks, so
    FreeSWITCH receives the speech ahead of time and plays it in a few
    playbacks instead of one per chunk. Every playback that starts makes
    FreeSWITCH drop 30-40 ms of the caller's audio, so fewer playbacks let the
    bot hear the caller while it speaks.

    Frames are passed on unchanged: the transport still paces them, so
    ``BotStartedSpeakingFrame``, ``BotStoppedSpeakingFrame`` and everything
    that depends on them keep their real-time meaning. Only the audio in the
    transport's chunks is no longer sent; the chunks become the clock.

    Get it from :meth:`FreeSWITCHFrameSerializer.lookahead` instead of
    building it directly::

        pipeline = Pipeline([..., tts, serializer.lookahead(), transport.output(), ...])

    Only audio that passes through this processor is played. Audio the
    transport produces on its own is not: the output mixer
    (``audio_out_mixer``), the end silence (``audio_out_end_silence_secs``)
    and the tones of ``OutputDTMFFrame``. The transport's output filter and
    volume do not apply either.
    """

    def __init__(self, serializer: "FreeSWITCHFrameSerializer", **kwargs):
        """Initialize the processor.

        Args:
            serializer: The serializer of the transport this processor feeds.
            **kwargs: Additional arguments passed to ``FrameProcessor``.
        """
        super().__init__(**kwargs)
        self._serializer = serializer
        self._warned_dtmf = False

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Hand bot audio to the serializer and pass every frame on.

        Args:
            frame: The frame to process.
            direction: The direction of frame flow in the pipeline.
        """
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, StartFrame):
                self._serializer._lookahead_started()
            elif isinstance(frame, InterruptionFrame):
                self._serializer._lookahead_interrupted()
            elif isinstance(frame, OutputAudioRawFrame):
                # The transport only writes audio for its default destination.
                if frame.transport_destination is None:
                    await self._serializer._lookahead_audio(frame)
            elif isinstance(frame, (OutputDTMFFrame, OutputDTMFUrgentFrame)):
                if not self._warned_dtmf:
                    self._warned_dtmf = True
                    logger.warning(
                        f"{self}: the transport's DTMF tones are not played with lookahead"
                    )

        elif isinstance(frame, BotStoppedSpeakingFrame):
            self._serializer._lookahead_bot_stopped()

        await self.push_frame(frame, direction)
