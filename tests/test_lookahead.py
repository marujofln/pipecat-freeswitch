#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""The lookahead processor and the growing blocks the serializer sends with it.

The processor runs in a real (one-processor) pipeline through Pipecat's ``run_test``. The transport
is played by the tests: every ``tick`` is one chunk it writes, 40 ms of audio that is not the bot's.
"""

import array
import base64
import json

import pytest
from loguru import logger
from pipecat.audio.dtmf.types import KeypadEntry
from pipecat.frames.frames import (
    BotStoppedSpeakingFrame,
    CancelFrame,
    Frame,
    InterruptionFrame,
    OutputAudioRawFrame,
    OutputDTMFFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.tests.utils import run_test

from pipecat_freeswitch import FreeSWITCHFrameSerializer, FreeSWITCHLookahead
from test_serializer import SETUP, UUID, make_serializer

CHUNK_MS = 40
CHUNK = 640  # bytes of one 40 ms chunk at 8 kHz
MARGIN = 1600  # bytes of the default lookahead_margin_secs (100 ms) at 8 kHz


def bot_audio(ms: int, rate: int = 8000, sample: int = 0x0101) -> OutputAudioRawFrame:
    audio = array.array("h", [sample] * (rate * ms // 1000)).tobytes()
    return OutputAudioRawFrame(audio=audio, sample_rate=rate, num_channels=1)


def tick(ms: int = CHUNK_MS, rate: int = 8000) -> OutputAudioRawFrame:
    """A chunk written by the transport. Its audio must never be sent."""
    return bot_audio(ms, rate, sample=0x7F7F)


async def feed(
    serializer: FreeSWITCHFrameSerializer,
    *frames: Frame,
    direction: FrameDirection = FrameDirection.DOWNSTREAM,
):
    """Run frames through the serializer's lookahead processor, as a pipeline would."""
    return await run_test(
        serializer.lookahead(), frames_to_send=list(frames), frames_to_send_direction=direction
    )


def sent(message: str | bytes | None) -> bytes:
    assert isinstance(message, str)
    return base64.b64decode(json.loads(message)["data"]["audioData"])


@pytest.fixture
async def serializer(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    yield serializer
    await serializer.serialize(CancelFrame())


@pytest.fixture
def warnings():
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    yield messages
    logger.remove(sink)


async def test_lookahead_is_always_the_same_processor(serializer):
    assert isinstance(serializer.lookahead(), FreeSWITCHLookahead)
    assert serializer.lookahead() is serializer.lookahead()


async def test_frames_pass_through_unchanged(serializer):
    frames = [bot_audio(20), bot_audio(20), InterruptionFrame(), bot_audio(20)]
    down, _ = await feed(serializer, *frames)
    assert [frame for frame in down if isinstance(frame, OutputAudioRawFrame)] == [
        frames[0],
        frames[1],
        frames[3],
    ]
    assert any(isinstance(frame, InterruptionFrame) for frame in down)


async def test_processor_left_out_of_the_pipeline_changes_nothing(serializer):
    serializer.lookahead()
    assert sent(await serializer.serialize(tick())) == tick().audio


async def test_first_chunk_sends_all_the_audio_produced_so_far(serializer):
    await feed(serializer, *[bot_audio(20)] * 10)
    assert sent(await serializer.serialize(tick())) == bot_audio(200).audio


async def test_transport_chunks_only_tell_the_time(serializer):
    await feed(serializer, bot_audio(40))
    assert sent(await serializer.serialize(tick())) == bot_audio(40).audio
    # The same audio, as the transport writes it: already sent.
    assert await serializer.serialize(tick()) is None


async def test_next_block_waits_until_the_audio_sent_is_about_to_run_out(serializer):
    await feed(serializer, bot_audio(1000))
    assert len(sent(await serializer.serialize(tick()))) == 16000
    await feed(serializer, bot_audio(200))
    # 960 ms ahead of the transport after the first chunk: the next block leaves when less
    # than the 100 ms margin is left, 22 chunks later.
    waited = 0
    while (message := await serializer.serialize(tick())) is None:
        waited += 1
    assert waited == 21
    assert sent(message) == bot_audio(200).audio


async def test_margin_is_configurable(esl_server):
    serializer = make_serializer(esl_server, lookahead_margin_secs=0.5)
    await serializer.setup(SETUP)
    await feed(serializer, bot_audio(1000))
    await serializer.serialize(tick())
    await feed(serializer, bot_audio(200))
    waited = 0
    while await serializer.serialize(tick()) is None:
        waited += 1
    assert waited == 11  # 960 ms ahead, 500 ms margin
    await serializer.serialize(CancelFrame())


async def test_blocks_grow_while_the_bot_speaks(serializer):
    # A source three times faster than real time: 3 s of speech produced in 1 s.
    blocks: list[int] = []
    produced = written = 0
    for chunk in range(75):
        if chunk < 25:
            await feed(serializer, bot_audio(3 * CHUNK_MS))
            produced += 3 * CHUNK
        message = await serializer.serialize(tick())
        written += CHUNK
        if message:
            blocks.append(len(sent(message)))
        # Never behind what the transport alone would have sent by now.
        assert sum(blocks) >= min(written, produced)
    assert sum(blocks) == 3 * 16000
    assert blocks[0] == 3 * CHUNK
    assert len(blocks) <= 8  # against 75 chunks
    assert max(blocks) >= 16000


async def test_audio_shorter_than_a_chunk_is_not_left_behind(esl_server):
    # With no margin at all, one chunk of margin still applies: the transport can hold back
    # up to a chunk, so its last chunk may come while the audio sent is still ahead of it.
    serializer = make_serializer(esl_server, lookahead_margin_secs=0)
    await serializer.setup(SETUP)
    await feed(serializer, bot_audio(100))
    assert len(sent(await serializer.serialize(tick()))) == 1600
    await feed(serializer, bot_audio(10))
    assert len(sent(await serializer.serialize(tick()))) == 160
    await serializer.serialize(CancelFrame())


async def test_audio_made_by_the_transport_takes_no_time(serializer):
    # Padding, end silence: chunks the transport writes with audio that never passed the
    # processor. They must not count as time the bot's audio has to catch up with.
    await feed(serializer)
    for _ in range(50):
        assert await serializer.serialize(tick()) is None
    await feed(serializer, bot_audio(1000))
    assert len(sent(await serializer.serialize(tick()))) == 16000
    await feed(serializer, bot_audio(200))
    # Still almost a second ahead: the next block waits.
    assert await serializer.serialize(tick()) is None


async def test_block_size_is_capped(serializer):
    await feed(serializer, bot_audio(6000))
    assert len(sent(await serializer.serialize(tick()))) == 5 * 16000
    while (message := await serializer.serialize(tick())) is None:
        pass
    assert len(sent(message)) == 16000


async def test_lookahead_audio_is_resampled_and_scaled(esl_server):
    serializer = make_serializer(esl_server, output_gain_db=-6.0206)
    await serializer.setup(SETUP)
    await feed(serializer, bot_audio(40, rate=16000, sample=1000))
    samples = array.array("h", sent(await serializer.serialize(tick(rate=16000))))
    assert 318 <= len(samples) <= 320  # 40 ms at 8 kHz
    assert all(abs(sample - 500) <= 2 for sample in samples[40:-40])
    await serializer.serialize(CancelFrame())


async def test_audio_before_the_uuid_is_held(esl_server):
    serializer = make_serializer(esl_server, call_uuid=None)
    await serializer.setup(SETUP)
    await feed(serializer, bot_audio(100))
    assert await serializer.serialize(tick()) is None
    await serializer.deserialize(UUID)
    assert sent(await serializer.serialize(tick())) == bot_audio(100).audio
    await serializer.serialize(CancelFrame())


async def test_interruption_drops_the_audio_not_sent_yet(serializer, esl_server):
    await feed(serializer, bot_audio(1000))
    await serializer.serialize(tick())
    await feed(serializer, bot_audio(1000))
    await feed(serializer, InterruptionFrame())
    await serializer.serialize(InterruptionFrame())
    await esl_server.wait_for_command(f"api uuid_break {UUID} all")
    assert await serializer.serialize(tick()) is None
    # The audio sent before the interruption no longer counts as ahead of the transport.
    await feed(serializer, bot_audio(40, sample=0x0202))
    assert sent(await serializer.serialize(tick())) == bot_audio(40, sample=0x0202).audio


async def test_chunks_from_before_an_interruption_send_nothing(serializer):
    # The processor sees the interruption first; the transport may still be writing a chunk
    # from before it, and the answer to the caller may already be on its way.
    await feed(serializer, bot_audio(200))
    await feed(serializer, InterruptionFrame())
    await feed(serializer, bot_audio(40, sample=0x0202))
    assert await serializer.serialize(tick()) is None
    await serializer.serialize(InterruptionFrame())
    assert sent(await serializer.serialize(tick())) == bot_audio(40, sample=0x0202).audio


async def test_interruption_that_skipped_the_processor_still_drops_audio(serializer):
    await feed(serializer, bot_audio(200))
    await serializer.serialize(InterruptionFrame())
    assert await serializer.serialize(tick()) is None


async def test_bot_stopped_speaking_starts_the_count_over(serializer):
    await feed(serializer, bot_audio(1000))
    assert len(sent(await serializer.serialize(tick()))) == 16000
    # The transport ends the utterance without writing the rest of it.
    await feed(serializer, BotStoppedSpeakingFrame(), direction=FrameDirection.UPSTREAM)
    await feed(serializer, bot_audio(40, sample=0x0202))
    assert sent(await serializer.serialize(tick())) == bot_audio(40, sample=0x0202).audio


async def test_audio_for_another_destination_is_ignored(serializer):
    frame = bot_audio(40)
    frame.transport_destination = "other"
    await feed(serializer, frame)
    assert await serializer.serialize(tick()) is None


async def test_warns_once_when_the_processor_sees_no_audio(serializer, warnings):
    await feed(serializer)  # in the pipeline, but not where the bot audio passes
    assert await serializer.serialize(tick()) is None
    assert await serializer.serialize(tick()) is None
    assert sum("never saw" in message for message in warnings) == 1


async def test_silence_from_the_transport_does_not_warn(serializer, warnings):
    await feed(serializer)
    assert await serializer.serialize(bot_audio(40, sample=0)) is None
    assert not any("never saw" in message for message in warnings)


async def test_warns_once_that_dtmf_tones_are_not_played(serializer, warnings):
    await feed(serializer, OutputDTMFFrame(button=KeypadEntry.ONE))
    await feed(serializer, OutputDTMFFrame(button=KeypadEntry.TWO))
    assert sum("DTMF" in message for message in warnings) == 1
