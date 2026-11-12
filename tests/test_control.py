#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio

import pytest
from loguru import logger

import pipecat_freeswitch.control
from fake_esl import FakeESLServer, play_path
from pipecat_freeswitch import FreeSWITCHControl

UUID = "11111111-2222-3333-4444-555555555555"


@pytest.fixture
async def control(esl_server):
    control = FreeSWITCHControl(port=esl_server.port)
    yield control
    await control.stop()


async def started(control) -> FreeSWITCHControl:
    await control.start(UUID)
    return control


async def test_calls_before_start_do_nothing(esl_server, control):
    await control.interrupt(3)
    await control.hang_up()
    assert await control.wait_for_playback(3, timeout=0.1)
    assert esl_server.commands == []


async def test_start_twice_opens_one_connection(esl_server, control):
    await started(control)
    await control.start(UUID)
    assert len([c for c in esl_server.commands if c.startswith("filter")]) == 1


async def test_failed_hang_up_does_not_raise(esl_server, control):
    esl_server.errors["api uuid_kill"] = "-ERR No such channel!"
    await started(control)
    await control.hang_up()
    assert f"api uuid_kill {UUID}" in esl_server.commands


async def test_failed_playback_is_not_waited_for(esl_server, control):
    esl_server.errors["sendmsg"] = "-ERR invalid session id"
    await started(control)
    await esl_server.emit_play(UUID, 0)
    await esl_server.wait_for_command("sendmsg")
    # The file will never play, so it must not hold up the hang-up.
    assert await control.wait_for_playback(1, timeout=1)


async def test_malformed_play_event_is_ignored(esl_server, control):
    await started(control)
    for body in ("not json", '{"no_file": 1}', "[1, 2]"):
        await esl_server.emit(
            {"Event-Name": "CUSTOM", "Event-Subclass": "mod_audio_stream::play"}, body
        )
    await esl_server.emit_play(UUID, 0)
    await esl_server.wait_for_command("sendmsg")
    sendmsgs = [c for c in esl_server.commands if c.startswith("sendmsg")]
    assert len(sendmsgs) == 1 and play_path(UUID, 0) in sendmsgs[0]


async def test_lost_connection_ends_the_wait_and_stops_commands(esl_server, control):
    await started(control)
    await esl_server.emit_play(UUID, 0)
    await esl_server.wait_for_command("sendmsg")
    wait = asyncio.create_task(control.wait_for_playback(1, timeout=5))
    await asyncio.sleep(0.05)
    await esl_server.disconnect()
    # Nothing can play any more: waiting would only delay the end of the call.
    assert await asyncio.wait_for(wait, 1)
    count = len(esl_server.commands)
    await control.interrupt(1)
    await control.hang_up()
    assert len(esl_server.commands) == count


async def test_interrupt_survives_a_dropped_connection(esl_server, control):
    esl_server.drop_on = "api uuid_break"
    await started(control)
    await esl_server.emit_play(UUID, 0)
    await esl_server.wait_for_command("sendmsg")
    await asyncio.wait_for(control.interrupt(1), 2)
    assert await control.wait_for_playback(1, timeout=1)


async def test_hang_up_survives_a_dropped_connection(esl_server, control):
    esl_server.drop_on = "api uuid_kill"
    await started(control)
    await asyncio.wait_for(control.hang_up(), 2)
    assert f"api uuid_kill {UUID}" in esl_server.commands


async def test_channel_hang_up_closes_the_connection(esl_server, control):
    # The application may never call stop(): the connection must not outlive the channel.
    await started(control)
    await esl_server.emit_hangup(UUID)
    await esl_server.wait_until_all_closed()
    await control.hang_up()
    assert not any(c.startswith("api uuid_kill") for c in esl_server.commands)


@pytest.fixture
def warnings(monkeypatch):
    monkeypatch.setattr(pipecat_freeswitch.control, "_warned_default_password", False)
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    yield messages
    logger.remove(sink)


def password_warnings(messages: list[str]) -> int:
    return sum("default event socket password" in m for m in messages)


async def test_default_password_warns_once_per_process(esl_server, warnings):
    for _ in range(2):
        control = FreeSWITCHControl(port=esl_server.port)
        await control.start(UUID)
        await control.stop()
    assert password_warnings(warnings) == 1


async def test_dedicated_password_does_not_warn(warnings):
    server = FakeESLServer(password="s3cret")
    await server.start()
    control = FreeSWITCHControl(port=server.port, password="s3cret")
    await control.start(UUID)
    await control.stop()
    await server.stop()
    assert password_warnings(warnings) == 0


async def test_playback_of_other_applications_is_ignored(esl_server, control):
    await started(control)
    await esl_server.emit_play(UUID, 0)
    await esl_server.wait_for_command("sendmsg")
    await esl_server.emit(
        {
            "Event-Name": "CHANNEL_EXECUTE_COMPLETE",
            "Unique-ID": UUID,
            "Application": "park",
            "Application-Data": play_path(UUID, 0),
        }
    )
    assert not await control.wait_for_playback(1, timeout=0.2)
    await esl_server.emit_playback_complete(UUID, 0)
    assert await control.wait_for_playback(1, timeout=1)


def send_texts(esl_server) -> list[str]:
    return [c for c in esl_server.commands if c.startswith("api uuid_audio_stream")]


async def test_subscribes_to_dtmf(esl_server, control):
    await started(control)
    assert "DTMF" in (await esl_server.wait_for_command("event")).split()


@pytest.mark.parametrize("digit", ["0", "5", "9", "*", "#", "A", "d"])
async def test_dtmf_is_forwarded_to_the_websocket(esl_server, control, digit):
    await started(control)
    await esl_server.emit_dtmf(UUID, digit)
    command = await esl_server.wait_for_command("api uuid_audio_stream")
    # No spaces: mod_audio_stream splits its API arguments on them.
    assert command == (
        f'api uuid_audio_stream {UUID} send_text {{"event":"dtmf","digit":"{digit.upper()}"}}'
    )


@pytest.mark.parametrize("digit", ["", "12", "E", "5\napi system id", "5 x", "'", '"'])
async def test_unexpected_dtmf_digit_is_not_forwarded(esl_server, control, digit):
    # The digit goes into an event socket command: anything but one key must stay out of it.
    await started(control)
    await esl_server.emit_dtmf(UUID, digit)
    await esl_server.emit_dtmf(UUID, "1")
    await esl_server.wait_for_command("api uuid_audio_stream")
    assert send_texts(esl_server) == [
        f'api uuid_audio_stream {UUID} send_text {{"event":"dtmf","digit":"1"}}'
    ]


async def test_failed_dtmf_forward_does_not_stop_the_control(esl_server, control):
    esl_server.errors["api uuid_audio_stream"] = "-ERR Operation Failed"
    await started(control)
    await esl_server.emit_dtmf(UUID, "1")
    await esl_server.wait_for_command("api uuid_audio_stream")
    await esl_server.emit_play(UUID, 0)
    await esl_server.wait_for_command("sendmsg")


async def test_playback_queued_during_an_interruption_is_still_waited_for(esl_server, control):
    await started(control)
    await esl_server.emit_play(UUID, 0)
    await esl_server.wait_for_command("sendmsg")
    esl_server.hold["api uuid_break"] = asyncio.Event()
    interruption = asyncio.create_task(control.interrupt(1))
    await esl_server.wait_for_command("api uuid_break")
    # Audio sent after the interruption is announced before the break is answered, so its
    # playback is queued right behind the break: forgetting the interrupted playback must not
    # forget the new one.
    await esl_server.emit_play(UUID, 1)
    await asyncio.sleep(0.05)
    esl_server.hold["api uuid_break"].set()
    await interruption
    async with asyncio.timeout(2):
        while sum(command.startswith("sendmsg") for command in esl_server.commands) < 2:
            await asyncio.sleep(0.005)
    assert not await control.wait_for_playback(2, timeout=0.2)
    await esl_server.emit_playback_complete(UUID, 1)
    assert await control.wait_for_playback(2, timeout=1)
