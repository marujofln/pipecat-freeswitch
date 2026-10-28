#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""End-to-end checks against the FreeSWITCH bench, driven through pytest.

``bench/run_e2e.py`` places real SIP/RTP calls through FreeSWITCH to ``bench/tone_bot.py``; these
tests run it once and check its results. Deselected by default; with the bench up
(``docker compose up -d`` in ``examples/freeswitch``) and the bot running
(``.venv/bin/python bench/tone_bot.py``), run them with ``pytest -m bench``. They are skipped when
either is not reachable.
"""

import json
import socket
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
ESL = ("127.0.0.1", 8021)
BOT = ("127.0.0.1", 8766)


def reachable(address: tuple[str, int]) -> bool:
    try:
        with socket.create_connection(address, timeout=0.5):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.bench,
    pytest.mark.skipif(
        not (reachable(ESL) and reachable(BOT)),
        reason="bench not running (FreeSWITCH event socket on 8021 and bench/tone_bot.py on 8766)",
    ),
]


@pytest.fixture(scope="module")
def results() -> dict[str, dict]:
    """Run the end-to-end scenarios once (PCMU, 40 ms blocks) and index the results by scenario."""
    run = subprocess.run(
        [sys.executable, str(REPO / "bench/run_e2e.py"), "--chunks", "4"],
        capture_output=True,
        text=True,
        timeout=300,
        check=True,
    )
    by_scenario = {}
    for line in run.stdout.splitlines():
        if line.startswith("{"):
            result = json.loads(line)
            by_scenario[result["scenario"]] = result
    return by_scenario


def test_handshake_without_the_token_is_refused(results):
    assert results["auth"]["refused"]


def test_speech_is_heard_whole_then_the_bot_hangs_up(results):
    speak = results["speak_end"]
    assert speak["bot_hung_up"] and speak["hangup_cause"] == "NORMAL_CLEARING"
    assert speak["glitch_windows"] == 0
    # The bench's receiving leg loses ~40 ms at the start of each talkspurt (documented), no more.
    assert speak["missing_ms"] <= 100
    # The hang-up waits for the last block to play, but not much longer.
    assert 0 <= speak["last_audio_to_hangup_ms"] < 500


def test_interruption_stops_the_audio_for_good(results):
    interrupt = results["interrupt"]
    assert not interrupt["audio_resumed"]
    assert interrupt["playbacks_started_after_interrupt"] == 0
    assert interrupt["heard_after_interrupt_ms"] < 100
