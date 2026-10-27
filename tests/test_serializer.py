#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio
import base64
import json
from types import SimpleNamespace

import pytest
from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    InputAudioRawFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    OutputTransportMessageFrame,
)

from fake_esl import play_path
from pipecat_freeswitch import FreeSWITCHControl, FreeSWITCHFrameSerializer

UUID = "11111111-2222-3333-4444-555555555555"

# Only audio_in_sample_rate is read by the serializer.
SETUP = SimpleNamespace(audio_in_sample_rate=16000)


def make_serializer(esl_server, call_uuid=UUID, **params) -> FreeSWITCHFrameSerializer:
    return FreeSWITCHFrameSerializer(
        call_uuid=call_uuid,
        esl_port=esl_server.port,
        params=FreeSWITCHFrameSerializer.InputParams(**params),
    )


def silence(ms: int, rate: int) -> bytes:
    return b"\x00\x00" * (rate * ms // 1000)


def audio_frame(ms: int = 20, rate: int = 8000) -> OutputAudioRawFrame:
    return OutputAudioRawFrame(audio=silence(ms, rate), sample_rate=rate, num_channels=1)


async def test_setup_follows_known_channel(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    assert await esl_server.wait_for_command("filter Unique-ID") == f"filter Unique-ID {UUID}"
    subscribe = await esl_server.wait_for_command("event plain")
    assert "CUSTOM mod_audio_stream::play" in subscribe
    await serializer.serialize(CancelFrame())


async def test_concurrent_setup_opens_one_connection(esl_server):
    # The input and output transports both call setup(), possibly at the same time.
    serializer = make_serializer(esl_server)
    await asyncio.gather(serializer.setup(SETUP), serializer.setup(SETUP))
    assert [c for c in esl_server.commands if c.startswith("filter")] == [
        f"filter Unique-ID {UUID}"
    ]
    await serializer.serialize(audio_frame())
    await esl_server.emit_play(UUID, 0)
    await esl_server.wait_for_command("sendmsg")
    await asyncio.sleep(0.1)
    assert len([c for c in esl_server.commands if c.startswith("sendmsg")]) == 1
    await serializer.serialize(CancelFrame())


async def test_uuid_from_first_text_message(esl_server):
    serializer = make_serializer(esl_server, call_uuid=None)
    await serializer.setup(SETUP)
    assert await serializer.deserialize(f"  {UUID}\n") is None
    assert serializer.call_uuid == UUID
    await esl_server.wait_for_command(f"filter Unique-ID {UUID}")
    await serializer.serialize(CancelFrame())


async def test_uuid_from_json_metadata(esl_server):
    serializer = make_serializer(esl_server, call_uuid=None)
    await serializer.setup(SETUP)
    await serializer.deserialize(json.dumps({"uuid": UUID, "lang": "pt-BR"}))
    assert serializer.call_uuid == UUID
    await serializer.serialize(CancelFrame())


async def test_caller_audio_is_resampled_to_pipeline_rate(esl_server):
    serializer = make_serializer(esl_server, freeswitch_sample_rate=8000)
    await serializer.setup(SETUP)
    frames = [await serializer.deserialize(silence(20, 8000)) for _ in range(50)]
    frames = [f for f in frames if f is not None]
    assert frames and all(isinstance(f, InputAudioRawFrame) for f in frames)
    assert all(f.sample_rate == 16000 and f.num_channels == 1 for f in frames)
    # 1 s at 8 kHz in, about 1 s at 16 kHz out: the stream resampler releases audio in
    # bursts and holds back up to ~150 ms.
    total = sum(len(f.audio) for f in frames)
    assert 32000 - 2 * 16 * 150 <= total <= 32000
    await serializer.serialize(CancelFrame())


async def test_bot_audio_becomes_stream_audio(esl_server):
    serializer = make_serializer(esl_server, freeswitch_sample_rate=8000)
    await serializer.setup(SETUP)
    message = json.loads(await serializer.serialize(audio_frame(40, 8000)))
    assert message["type"] == "streamAudio"
    assert message["data"]["audioDataType"] == "raw"
    assert message["data"]["sampleRate"] == 8000
    assert len(base64.b64decode(message["data"]["audioData"])) == 640
    await serializer.serialize(CancelFrame())


async def test_bot_audio_is_resampled_to_freeswitch_rate(esl_server):
    serializer = make_serializer(esl_server, freeswitch_sample_rate=16000)
    await serializer.setup(SETUP)
    sizes = []
    for _ in range(25):
        payload = await serializer.serialize(audio_frame(40, 24000))
        if payload is None:  # the stream resampler releases audio in bursts
            continue
        message = json.loads(payload)
        assert message["data"]["sampleRate"] == 16000
        sizes.append(len(base64.b64decode(message["data"]["audioData"])))
    # 1 s at 16 kHz is 32000 bytes, minus up to ~150 ms held by the resampler.
    assert 32000 - 2 * 16 * 150 <= sum(sizes) <= 32000
    await serializer.serialize(CancelFrame())


async def test_audio_before_uuid_is_held_and_sent_with_next_block(esl_server):
    serializer = make_serializer(esl_server, call_uuid=None)
    await serializer.setup(SETUP)
    assert await serializer.serialize(audio_frame(20)) is None
    assert await serializer.serialize(audio_frame(20)) is None
    await serializer.deserialize(UUID)
    message = json.loads(await serializer.serialize(audio_frame(20)))
    assert len(base64.b64decode(message["data"]["audioData"])) == 3 * 320
    await serializer.serialize(CancelFrame())


async def test_unknown_frames_and_messages_are_ignored(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    assert await serializer.serialize(OutputTransportMessageFrame(message={"x": 1})) is None
    assert await serializer.deserialize('{"event": "something"}') is None
    await serializer.serialize(CancelFrame())


def test_rejects_unsupported_sample_rate():
    with pytest.raises(ValueError):
        FreeSWITCHFrameSerializer(
            params=FreeSWITCHFrameSerializer.InputParams(freeswitch_sample_rate=11025)
        )


async def test_play_event_queues_playback_with_sendmsg(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    await serializer.serialize(audio_frame())
    await esl_server.emit_play(UUID, 0)
    command = await esl_server.wait_for_command("sendmsg")
    assert command == (
        f"sendmsg {UUID}\ncall-command: execute\nexecute-app-name: playback\n"
        f"execute-app-arg: {play_path(UUID, 0)}\nevent-lock: true"
    )
    assert "uuid_broadcast" not in " ".join(esl_server.commands)
    await serializer.serialize(CancelFrame())


async def test_interruption_breaks_playback_and_drops_stale_files(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    for _ in range(3):
        await serializer.serialize(audio_frame())
    assert await serializer.serialize(InterruptionFrame()) is None
    assert await esl_server.wait_for_command("api uuid_break") == f"api uuid_break {UUID} all"

    # play events for messages 0-2 were still in flight: they must not be played.
    for index in range(3):
        await esl_server.emit_play(UUID, index)
    # Message 3 was sent after the interruption: it must be played.
    await serializer.serialize(audio_frame())
    await esl_server.emit_play(UUID, 3)
    await esl_server.wait_for_command("sendmsg")
    sendmsgs = [c for c in esl_server.commands if c.startswith("sendmsg")]
    assert len(sendmsgs) == 1 and play_path(UUID, 3) in sendmsgs[0]
    await serializer.serialize(CancelFrame())


async def test_end_frame_waits_for_playback_before_hanging_up(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    for _ in range(2):
        await serializer.serialize(audio_frame())
    end = asyncio.create_task(serializer.serialize(EndFrame()))

    await esl_server.emit_play(UUID, 0)
    await esl_server.emit_play(UUID, 1)
    await asyncio.sleep(0.2)
    assert not end.done(), "hung up while audio was still queued"
    assert not any(c.startswith("api uuid_kill") for c in esl_server.commands)

    await esl_server.emit_playback_complete(UUID, 0)
    await esl_server.emit_playback_complete(UUID, 1)
    assert await asyncio.wait_for(end, 2) is None
    assert f"api uuid_kill {UUID}" in esl_server.commands


async def test_end_frame_hangs_up_after_timeout(esl_server):
    serializer = make_serializer(esl_server, hang_up_timeout_secs=0.2)
    await serializer.setup(SETUP)
    await serializer.serialize(audio_frame())
    await asyncio.wait_for(serializer.serialize(EndFrame()), 2)
    assert f"api uuid_kill {UUID}" in esl_server.commands


async def test_cancel_frame_hangs_up_immediately(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    await serializer.serialize(audio_frame())
    await esl_server.emit_play(UUID, 0)
    await esl_server.wait_for_command("sendmsg")
    await asyncio.wait_for(serializer.serialize(CancelFrame()), 1)
    assert f"api uuid_kill {UUID}" in esl_server.commands


async def test_no_hang_up_when_disabled(esl_server):
    serializer = make_serializer(esl_server, auto_hang_up=False)
    await serializer.setup(SETUP)
    await serializer.serialize(EndFrame())
    assert not any(c.startswith("api uuid_kill") for c in esl_server.commands)


async def test_caller_hang_up_ends_the_wait(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    await serializer.serialize(audio_frame())
    end = asyncio.create_task(serializer.serialize(EndFrame()))
    await esl_server.emit_play(UUID, 0)
    await esl_server.emit_hangup(UUID)
    await asyncio.wait_for(end, 2)
    assert not any(c.startswith("api uuid_kill") for c in esl_server.commands)


async def test_wrong_password_fails_setup(esl_server):
    serializer = FreeSWITCHFrameSerializer(
        call_uuid=UUID, esl_port=esl_server.port, esl_password="wrong"
    )
    with pytest.raises(Exception, match="authentication failed"):
        await serializer.setup(SETUP)


async def test_control_can_be_injected(esl_server):
    control = FreeSWITCHControl(port=esl_server.port)
    serializer = FreeSWITCHFrameSerializer(call_uuid=UUID, control=control)
    await serializer.setup(SETUP)
    assert control.started and control.uuid == UUID
    await serializer.serialize(CancelFrame())
    assert not control.started
