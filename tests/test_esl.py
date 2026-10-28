#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio

import pytest

from pipecat_freeswitch import ESLConnection, ESLError
from pipecat_freeswitch.esl import parse_plain_event


async def connect(esl_server, password: str = "ClueCon") -> ESLConnection:
    return await ESLConnection.connect("127.0.0.1", esl_server.port, password, timeout=2)


async def test_api_returns_the_reply_body(esl_server):
    esl_server.errors["api uuid_kill"] = "-ERR No such channel!"
    conn = await connect(esl_server)
    assert await conn.api("uuid_kill x") == "-ERR No such channel!"
    assert await conn.api("status") == "+OK"
    await conn.close()


async def test_unexpected_greeting_is_refused(esl_server):
    esl_server.greeting = "text/rude-rejection"
    with pytest.raises(ESLError, match="unexpected greeting"):
        await connect(esl_server)


async def test_wrong_password_is_refused(esl_server):
    with pytest.raises(ESLError, match="authentication failed"):
        await connect(esl_server, password="wrong")


async def test_send_on_closed_connection_fails(esl_server):
    conn = await connect(esl_server)
    await conn.close()
    assert conn.closed
    with pytest.raises(ESLError, match="closed"):
        await conn.api("status")


@pytest.mark.parametrize(
    "notice", [pytest.param(True, id="disconnect-notice"), pytest.param(False, id="socket-closed")]
)
async def test_lost_connection_ends_events_and_commands(esl_server, notice):
    conn = await connect(esl_server)
    await esl_server.disconnect(notice=notice)
    # Every waiter is woken, not only the first one.
    for _ in range(2):
        with pytest.raises(ESLError):
            await asyncio.wait_for(conn.next_event(), 2)
    assert conn.closed
    with pytest.raises(ESLError):
        await conn.api("status")


async def test_command_waiting_for_its_reply_fails_when_the_connection_drops(esl_server):
    esl_server.drop_on = "api uuid_kill"
    conn = await connect(esl_server)
    with pytest.raises(ESLError):
        await asyncio.wait_for(conn.api("uuid_kill x"), 2)


async def test_events_and_replies_are_routed_apart(esl_server):
    conn = await connect(esl_server)
    await esl_server.emit({"Event-Name": "HEARTBEAT"})
    assert await conn.api("status") == "+OK"
    event = await asyncio.wait_for(conn.next_event(), 2)
    assert event["Event-Name"] == "HEARTBEAT"
    await conn.close()


@pytest.mark.parametrize(
    "body,expected",
    [
        pytest.param(
            "Event-Name: CUSTOM\nApplication-Data: %2Ftmp%2Fa%20b.r8\n\n",
            {"Event-Name": "CUSTOM", "Application-Data": "/tmp/a b.r8"},
            id="url-decoded-headers",
        ),
        pytest.param(
            'Event-Name: CUSTOM\nContent-Length: 13\n\n{"file": "x"}trailing',
            {"Event-Name": "CUSTOM", "Content-Length": "13", "_body": '{"file": "x"}'},
            id="body-cut-at-content-length",
        ),
        pytest.param(
            "Event-Name: CUSTOM\nnot a header\n\n",
            {"Event-Name": "CUSTOM"},
            id="malformed-line-skipped",
        ),
    ],
)
def test_parse_plain_event(body, expected):
    assert parse_plain_event(body) == expected
