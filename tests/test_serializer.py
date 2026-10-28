#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import array
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

# Only the sample rates are read by the serializer.
SETUP = SimpleNamespace(audio_in_sample_rate=16000, audio_out_sample_rate=8000)


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


INJECTED = "api system touch /tmp/pwned"


@pytest.mark.parametrize(
    "message",
    [
        pytest.param(f"x\n\n{INJECTED}", id="plain-text"),
        pytest.param(f"{UUID}\n\n{INJECTED}", id="plain-text-after-valid-uuid"),
        pytest.param(json.dumps({"uuid": f"x\n\n{INJECTED}"}), id="json"),
        pytest.param(json.dumps({"uuid": f"{UUID}\n\n{INJECTED}"}), id="json-after-valid-uuid"),
        pytest.param(f"{UUID} all", id="trailing-argument"),
        pytest.param("not-a-uuid", id="not-a-uuid"),
    ],
)
async def test_first_message_cannot_inject_esl_commands(esl_server, message):
    # Any WebSocket client can send the first text message, and the UUID goes into ESL commands.
    serializer = make_serializer(esl_server, call_uuid=None)
    await serializer.setup(SETUP)
    assert await serializer.deserialize(message) is None
    assert serializer.call_uuid is None
    await asyncio.sleep(0.1)
    assert esl_server.commands == []
    await serializer.serialize(CancelFrame())


@pytest.mark.parametrize(
    "message",
    [
        pytest.param("{not json", id="invalid-json"),
        pytest.param('{"lang": "pt-BR"}', id="json-without-uuid"),
        pytest.param('{"uuid": 42}', id="json-uuid-not-a-string"),
        pytest.param("   ", id="blank"),
    ],
)
async def test_first_message_without_uuid_is_ignored(esl_server, message):
    serializer = make_serializer(esl_server, call_uuid=None)
    await serializer.setup(SETUP)
    assert await serializer.deserialize(message) is None
    assert serializer.call_uuid is None
    assert esl_server.commands == []
    await serializer.serialize(CancelFrame())


async def test_valid_uuid_still_accepted_after_rejected_message(esl_server):
    serializer = make_serializer(esl_server, call_uuid=None)
    await serializer.setup(SETUP)
    await serializer.deserialize(f"x\n\n{INJECTED}")
    await serializer.deserialize(UUID)
    assert serializer.call_uuid == UUID
    await esl_server.wait_for_command(f"filter Unique-ID {UUID}")
    assert not any(INJECTED in c for c in esl_server.commands)
    await serializer.serialize(CancelFrame())


def test_rejects_malformed_call_uuid():
    with pytest.raises(ValueError, match="canonical UUID"):
        FreeSWITCHFrameSerializer(call_uuid=f"{UUID}\n\n{INJECTED}")


async def test_control_rejects_malformed_uuid(esl_server):
    control = FreeSWITCHControl(port=esl_server.port)
    with pytest.raises(ValueError, match="not a channel UUID"):
        await control.start(f"{UUID}\n\n{INJECTED}")
    assert not control.started
    assert esl_server.commands == []


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
    serializer = make_serializer(esl_server, freeswitch_out_sample_rate=16000)
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


async def test_audio_held_without_uuid_is_capped(esl_server):
    serializer = make_serializer(esl_server, call_uuid=None)
    await serializer.setup(SETUP)
    for _ in range(350):  # 7 s of 20 ms frames, with no UUID yet
        assert await serializer.serialize(audio_frame(20)) is None
    await serializer.deserialize(UUID)
    message = json.loads(await serializer.serialize(audio_frame(20)))
    # Only the last 5 s are kept (8 kHz L16), plus the new frame.
    assert len(base64.b64decode(message["data"]["audioData"])) == 5 * 16000 + 320
    await serializer.serialize(CancelFrame())


def tone_frame(amplitude: int, ms: int = 20, rate: int = 8000) -> OutputAudioRawFrame:
    samples = array.array(
        "h", [amplitude if i % 2 else -amplitude for i in range(rate * ms // 1000)]
    )
    return OutputAudioRawFrame(audio=samples.tobytes(), sample_rate=rate, num_channels=1)


def sent_samples(message: str) -> array.array:
    return array.array("h", base64.b64decode(json.loads(message)["data"]["audioData"]))


@pytest.mark.parametrize(
    "gain_db,amplitude,expected",
    [
        pytest.param(0.0, 20000, 20000, id="default-unchanged"),
        pytest.param(-6.0, 20000, 10023, id="attenuated"),
        pytest.param(12.0, 20000, 32767, id="clipped-to-16-bits"),
    ],
)
async def test_output_gain(esl_server, gain_db, amplitude, expected):
    serializer = make_serializer(esl_server, output_gain_db=gain_db)
    await serializer.setup(SETUP)
    samples = sent_samples(await serializer.serialize(tone_frame(amplitude)))
    assert max(samples) == expected
    await serializer.serialize(CancelFrame())


async def test_output_gain_applies_to_held_audio(esl_server):
    serializer = make_serializer(esl_server, call_uuid=None, output_gain_db=-6.0)
    await serializer.setup(SETUP)
    assert await serializer.serialize(tone_frame(20000)) is None  # held: no UUID yet
    await serializer.deserialize(UUID)
    samples = sent_samples(await serializer.serialize(tone_frame(20000)))
    assert max(samples) == 10023 and len(samples) == 2 * 160
    await serializer.serialize(CancelFrame())


async def test_unknown_frames_and_messages_are_ignored(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    assert await serializer.serialize(OutputTransportMessageFrame(message={"x": 1})) is None
    assert await serializer.deserialize('{"event": "something"}') is None
    await serializer.serialize(CancelFrame())


async def test_separate_in_and_out_rates(esl_server):
    # Opus channel: caller audio streamed at 16 kHz, bot audio sent at the channel's 48 kHz.
    serializer = make_serializer(
        esl_server, freeswitch_in_sample_rate=16000, freeswitch_out_sample_rate=48000
    )
    await serializer.setup(SimpleNamespace(audio_in_sample_rate=16000, audio_out_sample_rate=24000))
    frame = await serializer.deserialize(silence(20, 16000))
    assert frame.sample_rate == 16000 and len(frame.audio) == 640  # no resampling
    message = json.loads(await serializer.serialize(audio_frame(20, 48000)))
    assert message["data"]["sampleRate"] == 48000
    # FreeSWITCH cannot play raw audio at 48 kHz (.r48), so it goes as WAV.
    assert message["data"]["audioDataType"] == "wav"
    wav = base64.b64decode(message["data"]["audioData"])
    assert wav[:4] == b"RIFF" and wav[8:12] == b"WAVE" and len(wav) == 44 + 1920
    await serializer.serialize(CancelFrame())


async def test_output_rate_defaults_to_freeswitch_rate_not_pipeline_rate(esl_server):
    # A 24 kHz TTS on a G.711 channel: sending 24 kHz would make FreeSWITCH resample every block,
    # with an artefact at each seam, so the serializer resamples to 8 kHz itself.
    serializer = make_serializer(esl_server)
    await serializer.setup(SimpleNamespace(audio_in_sample_rate=16000, audio_out_sample_rate=24000))
    message = json.loads(await serializer.serialize(audio_frame(40, 24000)))
    assert message["data"]["sampleRate"] == 8000
    await serializer.serialize(CancelFrame())


async def test_output_rate_follows_freeswitch_sample_rate(esl_server):
    serializer = make_serializer(esl_server, freeswitch_sample_rate=16000)
    await serializer.setup(SimpleNamespace(audio_in_sample_rate=16000, audio_out_sample_rate=24000))
    message = json.loads(await serializer.serialize(audio_frame(40, 24000)))
    assert message["data"]["sampleRate"] == 16000
    await serializer.serialize(CancelFrame())


async def test_pipeline_rate_equal_to_output_rate_is_passed_through(esl_server):
    serializer = make_serializer(esl_server, freeswitch_out_sample_rate=24000)
    await serializer.setup(SimpleNamespace(audio_in_sample_rate=16000, audio_out_sample_rate=24000))
    message = json.loads(await serializer.serialize(audio_frame(40, 24000)))
    assert message["data"]["sampleRate"] == 24000 and message["data"]["audioDataType"] == "raw"
    assert len(base64.b64decode(message["data"]["audioData"])) == 1920  # passed through as is
    await serializer.serialize(CancelFrame())


async def test_unplayable_pipeline_rate_is_resampled(esl_server):
    serializer = make_serializer(esl_server, freeswitch_sample_rate=8000)
    await serializer.setup(SimpleNamespace(audio_in_sample_rate=16000, audio_out_sample_rate=22050))
    message = json.loads(await serializer.serialize(audio_frame(40, 22050)))
    assert message["data"]["sampleRate"] == 8000
    await serializer.serialize(CancelFrame())


async def test_resampling_holds_back_no_audio(esl_server):
    # Nothing flushes the resampler after the last frame of an utterance, so it must not keep any.
    serializer = make_serializer(esl_server, freeswitch_out_sample_rate=8000)
    await serializer.setup(SimpleNamespace(audio_in_sample_rate=16000, audio_out_sample_rate=24000))
    sent = 0
    for _ in range(25):  # 1 s of 40 ms frames at 24 kHz
        message = json.loads(await serializer.serialize(audio_frame(40, 24000)))
        sent += len(base64.b64decode(message["data"]["audioData"]))
    assert abs(sent - 16000) <= 2 * 8  # 1 s at 8 kHz, within a millisecond
    await serializer.serialize(CancelFrame())


def test_rejects_unsupported_input_rate():
    with pytest.raises(ValueError):
        FreeSWITCHFrameSerializer(
            params=FreeSWITCHFrameSerializer.InputParams(freeswitch_in_sample_rate=11025)
        )


@pytest.mark.parametrize(
    "params",
    [
        pytest.param({"freeswitch_out_sample_rate": 64000}, id="out-rate"),
        pytest.param({"freeswitch_sample_rate": 64000}, id="default-rate"),
    ],
)
def test_rejects_unsupported_sample_rate(params):
    with pytest.raises(ValueError):
        FreeSWITCHFrameSerializer(params=FreeSWITCHFrameSerializer.InputParams(**params))


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


def odd_path(index: int) -> str:
    """A file name that does not follow mod_audio_stream's <uuid>_<n>.tmp.<ext> scheme."""
    return f"/tmp/audio-{index}.r8"


async def test_unexpected_file_name_does_not_stall_hang_up(esl_server):
    serializer = make_serializer(esl_server, hang_up_timeout_secs=5)
    await serializer.setup(SETUP)
    for _ in range(2):
        await serializer.serialize(audio_frame())
    end = asyncio.create_task(serializer.serialize(EndFrame()))
    for index in range(2):
        await esl_server.emit_play(UUID, index, path=odd_path(index))
    await asyncio.sleep(0.1)
    assert len([c for c in esl_server.commands if c.startswith("sendmsg")]) == 2
    for index in range(2):
        await esl_server.emit_playback_complete(UUID, index, path=odd_path(index))
    # Without arrival-order numbering this waited for the 5 s timeout.
    assert await asyncio.wait_for(end, 1) is None
    assert f"api uuid_kill {UUID}" in esl_server.commands


async def test_unexpected_file_name_still_drops_stale_files(esl_server):
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    for _ in range(2):
        await serializer.serialize(audio_frame())
    await serializer.serialize(InterruptionFrame())
    await esl_server.wait_for_command("api uuid_break")
    for index in range(2):
        await esl_server.emit_play(UUID, index, path=odd_path(index))
    await serializer.serialize(audio_frame())
    await esl_server.emit_play(UUID, 2, path=odd_path(2))
    await esl_server.wait_for_command("sendmsg")
    await asyncio.sleep(0.1)
    sendmsgs = [c for c in esl_server.commands if c.startswith("sendmsg")]
    assert len(sendmsgs) == 1 and odd_path(2) in sendmsgs[0]
    await serializer.serialize(CancelFrame())


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


async def test_cleanup_hangs_up_and_closes_the_event_socket(esl_server):
    # The transport skips the serializer once the WebSocket is closed, so cleanup() ends the call.
    serializer = make_serializer(esl_server)
    await serializer.setup(SETUP)
    await serializer.cleanup()
    assert f"api uuid_kill {UUID}" in esl_server.commands
    await esl_server.wait_until_all_closed()


async def test_cleanup_after_end_frame_does_nothing_more(esl_server):
    serializer = make_serializer(esl_server, hang_up_timeout_secs=0.1)
    await serializer.setup(SETUP)
    await serializer.serialize(EndFrame())
    await serializer.cleanup()
    assert esl_server.commands.count(f"api uuid_kill {UUID}") == 1


async def test_cleanup_without_auto_hang_up_only_closes(esl_server):
    serializer = make_serializer(esl_server, auto_hang_up=False)
    await serializer.setup(SETUP)
    await serializer.cleanup()
    assert not any(c.startswith("api uuid_kill") for c in esl_server.commands)
    await esl_server.wait_until_all_closed()


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
