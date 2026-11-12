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


def run_e2e(*args: str) -> dict[str, dict]:
    """Run bench/run_e2e.py with 40 ms chunks (PCMU) and index the results by scenario."""
    run = subprocess.run(
        [sys.executable, str(REPO / "bench/run_e2e.py"), "--chunks", "4", *args],
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


@pytest.fixture(scope="module")
def results() -> dict[str, dict]:
    """Every end-to-end scenario, once, with the transport's fixed blocks."""
    return run_e2e()


@pytest.fixture(scope="module")
def lookahead_results() -> dict[str, dict]:
    """The audio scenarios with the lookahead processor and a tone produced at 3x real time."""
    return run_e2e(
        "--scenarios", "speak_end,listen,interrupt", "--lookahead", "on", "--tone-speed", "3"
    )


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


def test_caller_keys_reach_the_pipeline(results):
    dtmf = results["dtmf"]
    assert dtmf["received"] == dtmf["sent"]


def test_lookahead_speech_is_heard_whole_then_the_bot_hangs_up(lookahead_results):
    speak = lookahead_results["speak_end"]
    assert speak["bot_hung_up"] and speak["hangup_cause"] == "NORMAL_CLEARING"
    assert speak["glitch_windows"] == 0
    assert speak["missing_ms"] <= 100
    assert 0 <= speak["last_audio_to_hangup_ms"] < 500


def test_lookahead_plays_the_speech_in_a_few_growing_blocks(lookahead_results, results):
    listen = lookahead_results["listen"]
    # 3 s in 40 ms chunks: 75 playbacks with fixed blocks.
    assert results["listen"]["playbacks"] >= 70
    assert listen["playbacks"] <= 12
    assert max(listen["playback_ms"]) >= 500
    # No more holes in the bot's tone than with fixed blocks.
    assert listen["holes_heard"] <= results["listen"]["holes_heard"]


def test_lookahead_lets_the_bot_hear_the_caller(lookahead_results, results):
    # Every playback that starts flushes 30-40 ms of the caller's audio.
    assert results["listen"]["caller_audio_lost_pct"] > 80
    assert lookahead_results["listen"]["caller_audio_lost_pct"] < 25


def test_lookahead_interruption_stops_the_audio_for_good(lookahead_results):
    interrupt = lookahead_results["interrupt"]
    assert not interrupt["audio_resumed"]
    assert interrupt["playbacks_started_after_interrupt"] == 0
    assert interrupt["heard_after_interrupt_ms"] < 100
