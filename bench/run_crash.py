"""What happens to a call when the bot process dies mid-call.

Starts ``bench/tone_bot.py`` itself, places a SIP call to extension 9998 with the bot speaking (``listen``
scenario: a tone at real time, the call kept up), and then, about a second into the bot's audio:

- ``kill``: SIGKILL the bot. The kernel closes its TCP connections without a WebSocket CLOSE.
- ``stop``: SIGSTOP the bot. The connection stays open but nothing answers, as when the bot's host or its
  network vanishes. Only mod_audio_stream's pings (``STREAM_HEART_BEAT``, set in the dialplan) notice.
- ``none``: leave the bot alone; a control showing the pings do not end a healthy call.
- ``down``: no bot at all when the call arrives, so the WebSocket cannot be opened.

It reports when mod_audio_stream raised ``error`` or ``disconnect`` and when, and with what cause, the
channel hung up. With the bench's Lua hook (``conf/scripts/audio_stream_hangup.lua``) the call should end
right after the ``error`` event; without it the dialplan's ``park`` keeps the caller on the line.

    python3 bench/run_crash.py                         # kill, stop and none
    python3 bench/run_crash.py --modes kill --watch 60

Requires the bench (``docker compose up -d`` in ``examples/freeswitch``) and the repository's ``.venv``.
Do not run ``tone_bot.py`` at the same time: this script starts its own. Standard library only.
"""

import argparse
import asyncio
import json
import os
import signal
import subprocess
import time
import uuid as uuidlib

from esl import EslConnection
from run_e2e import BOT_URL, wait_for_hangup
from run_experiments import DIAL_STRINGS, REPO, SUBSCRIPTIONS, Bench, event_time, ms

TONE_SECONDS = 60.0  # longer than any watch window: the bot is still speaking when it dies


async def start_bot() -> subprocess.Popen:
    """Start tone_bot.py and wait until it accepts connections."""
    proc = subprocess.Popen(
        [str(REPO / ".venv/bin/python"), str(REPO / "bench/tone_bot.py")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    host, port = BOT_URL.removeprefix("http://").split(":")
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            _, writer = await asyncio.open_connection(host, int(port))
            writer.close()
            return proc
        except OSError:
            await asyncio.sleep(0.2)
    proc.kill()
    raise TimeoutError("tone_bot.py did not start")


def stop_bot(proc: subprocess.Popen) -> None:
    """Make sure the bot is gone (it may be stopped or already dead)."""
    if proc.poll() is None:
        os.kill(proc.pid, signal.SIGKILL)
    proc.wait()


async def run_down(bench: Bench, watch: float) -> dict:
    """Call with no bot listening: the stream fails to connect."""
    caller = str(uuidlib.uuid4())
    since = time.time()
    await bench.conn.api(
        f"originate {{origination_uuid={caller},absolute_codec_string={bench.codec}}}"
        f"{DIAL_STRINGS[bench.endpoint].format(extension=bench.extension)} &park()"
    )
    deadline = time.time() + 10
    errors = []
    while not errors and time.time() < deadline:
        errors = [
            e for e in bench.events if e.get("Event-Subclass") == "mod_audio_stream::error" and event_time(e) >= since
        ]
        await asyncio.sleep(0.05)
    leg = errors[0]["Unique-ID"] if errors else None
    hangup_at = await wait_for_hangup(bench, leg, watch) if leg else None
    hangup = bench.events_for(leg, "CHANNEL_HANGUP_COMPLETE") if leg else []
    await bench.conn.api(f"uuid_kill {caller}")
    await asyncio.sleep(1.0)
    error_at = event_time(errors[0]) if errors else None
    return {
        "scenario": "crash",
        "mode": "down",
        "leg_uuid": leg,
        "watched_s": watch,
        "error_event_ms": ms(error_at - since) if error_at else None,
        "error_body": errors[0].get("_body") if errors else None,
        "hung_up": hangup_at is not None,
        "error_to_hangup_ms": ms(hangup_at - error_at) if hangup_at and error_at else None,
        "hangup_cause": hangup[0].get("Hangup-Cause") if hangup else None,
    }


async def run_mode(bench: Bench, mode: str, watch: float) -> dict:
    """Place a call, end the bot as the mode says and watch the call for a while."""
    if mode == "down":
        return await run_down(bench, watch)
    bot = await start_bot()
    try:
        call = await bench.place_call(
            "bot",
            200,
            TONE_SECONDS,
            scenario="listen",
            audio_out_10ms_chunks=20,
            tone_speed=1.0,
            in_rate=8000,
            out_rate=8000,
            pipeline_out_rate=8000,
        )
        await bench.wait_first_playback(call)
        await asyncio.sleep(1.0)
        crashed_at = time.time()
        if mode == "kill":
            os.kill(bot.pid, signal.SIGKILL)
        elif mode == "stop":
            os.kill(bot.pid, signal.SIGSTOP)
        hangup_at = await wait_for_hangup(bench, call.leg_uuid, watch)
        errors = bench.events_for(call.leg_uuid, "mod_audio_stream::error", crashed_at)
        disconnects = bench.events_for(call.leg_uuid, "mod_audio_stream::disconnect", crashed_at)
        hangup = bench.events_for(call.leg_uuid, "CHANNEL_HANGUP_COMPLETE")
        await bench.hang_up(call)
    finally:
        stop_bot(bot)
    error_at = event_time(errors[0]) if errors else None
    return {
        "scenario": "crash",
        "mode": mode,
        "leg_uuid": call.leg_uuid,
        "watched_s": watch,
        "error_event_ms": ms(error_at - crashed_at) if error_at else None,
        "error_body": errors[0].get("_body") if errors else None,
        "disconnect_event_ms": ms(event_time(disconnects[0]) - crashed_at) if disconnects else None,
        "hung_up": hangup_at is not None,
        "crash_to_hangup_ms": ms(hangup_at - crashed_at) if hangup_at else None,
        "error_to_hangup_ms": ms(hangup_at - error_at) if hangup_at and error_at else None,
        "hangup_cause": hangup[0].get("Hangup-Cause") if hangup else None,
    }


async def main():
    """Run the modes and print the results."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--modes", default="kill,stop,none,down")
    parser.add_argument("--watch", type=float, default=30.0, help="seconds to wait for the hang-up")
    args = parser.parse_args()

    conn = await EslConnection.connect("127.0.0.1", 8021, "ClueCon")
    await conn.subscribe(SUBSCRIPTIONS)
    bench = Bench(
        conn=conn,
        echo_url=BOT_URL,
        captures=REPO / "examples/freeswitch/captures",
        endpoint="sip",
        extension="9998",
        codec="PCMU",
    )
    collector = asyncio.create_task(bench.collect_events())
    results = []
    try:
        for mode in args.modes.split(","):
            result = await run_mode(bench, mode, args.watch)
            results.append(result)
            print(json.dumps(result), flush=True)
    finally:
        collector.cancel()
        await conn.api("hupall")
        await conn.close()
    out = bench.captures / f"crash-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"results written to {out}")


if __name__ == "__main__":
    asyncio.run(main())
