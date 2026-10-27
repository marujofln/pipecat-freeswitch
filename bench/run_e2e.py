"""End-to-end check of FreeSWITCHFrameSerializer against the bench FreeSWITCH.

Places SIP/RTP calls to extension 9998, served by ``bench/tone_bot.py`` (a Pipecat pipeline using the
serializer), and checks what the caller hears:

- ``speak_end``: the bot plays a tone and ends the call. Expect the whole tone to be heard and the
  hang-up to come only after it.
- ``interrupt``: the bot is interrupted while the tone plays. Expect the audio to stop at once and not
  resume.

Requires the bench (``docker compose up -d`` in ``examples/freeswitch``) and the bot
(``.venv/bin/python bench/tone_bot.py``). Standard library only.
"""

import argparse
import asyncio
import json
import time

from esl import EslConnection
from run_experiments import REPO, SUBSCRIPTIONS, Bench, event_time, ms

BOT_URL = "http://127.0.0.1:8766"


def bot_log(bench: Bench, uuid: str) -> list[dict]:
    """Read tone_bot's log for a call."""
    path = bench.captures / f"{uuid}-bot.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def first(log: list[dict], kind: str) -> float | None:
    """Wall time of the first bot event of a kind."""
    return next((e["wall"] for e in log if e["kind"] == kind), None)


async def wait_for_hangup(bench: Bench, uuid: str, timeout: float) -> float | None:
    """Wait until the streaming leg hangs up; return the time it did."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if hangups := bench.events_for(uuid, "CHANNEL_HANGUP_COMPLETE"):
            return event_time(hangups[0])
        await asyncio.sleep(0.05)
    return None


async def run_speak_end(bench: Bench, tone_seconds: float, chunks: int) -> dict:
    """Bot plays a tone then queues EndFrame."""
    call = await bench.place_call(
        "bot", chunks * 10, tone_seconds, scenario="speak_end", audio_out_10ms_chunks=chunks
    )
    hangup_at = await wait_for_hangup(bench, call.leg_uuid, tone_seconds + 15)
    await bench.hang_up(call)
    log = bot_log(bench, call.leg_uuid)
    heard = bench.audible_spans(call)
    heard_s = sum(e - s for s, e in heard)
    hangup = bench.events_for(call.leg_uuid, "CHANNEL_HANGUP_COMPLETE")
    finished = next((e for e in log if e["kind"] == "finished"), {})
    end_queued = first(log, "end_queued")
    return {
        "scenario": "speak_end",
        "block_ms": chunks * 10,
        "leg_uuid": call.leg_uuid,
        "tone_s": tone_seconds,
        "heard_s": round(heard_s, 2),
        "missing_ms": ms(max(0.0, tone_seconds - heard_s)),
        "holes_heard": len(bench.holes(call)),
        "bot_hung_up": hangup_at is not None,
        "hangup_cause": hangup[0].get("Hangup-Cause") if hangup else None,
        "end_queued_to_hangup_ms": ms(hangup_at - end_queued) if hangup_at and end_queued else None,
        "last_audio_to_hangup_ms": ms(hangup_at - heard[-1][1]) if hangup_at and heard else None,
        "caller_audio_frames_in_pipeline": finished.get("input_frames"),
    }


async def run_interrupt(bench: Bench, tone_seconds: float, after_s: float, chunks: int) -> dict:
    """Bot is interrupted while playing a tone."""
    call = await bench.place_call(
        "bot",
        chunks * 10,
        tone_seconds,
        scenario="interrupt",
        interrupt_after_s=after_s,
        audio_out_10ms_chunks=chunks,
    )
    await wait_for_hangup(bench, call.leg_uuid, after_s + 15)
    await bench.hang_up(call)
    log = bot_log(bench, call.leg_uuid)
    interrupt_at = first(log, "interrupt")
    heard = bench.audible_spans(call)
    after = [(max(s, interrupt_at), e) for s, e in heard if interrupt_at and e > interrupt_at]
    return {
        "scenario": "interrupt",
        "block_ms": chunks * 10,
        "leg_uuid": call.leg_uuid,
        "tone_s": tone_seconds,
        "interrupt_after_s": after_s,
        "heard_before_interrupt_s": round(
            sum(min(e, interrupt_at) - s for s, e in heard if interrupt_at and s < interrupt_at), 2
        ),
        "heard_after_interrupt_ms": ms(sum(e - s for s, e in after)),
        "audio_resumed": len(after) > 1,
        "playbacks_started_after_interrupt": sum(
            1 for s, _ in bench.playbacks(call.leg_uuid) if interrupt_at and s > interrupt_at + 0.05
        ),
    }


async def main():
    """Run both scenarios and print the results."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--chunks", default="4,20", help="audio_out_10ms_chunks values (block = 10 ms each)")
    parser.add_argument("--tone-seconds", type=float, default=3.0)
    args = parser.parse_args()

    conn = await EslConnection.connect("127.0.0.1", 8021, "ClueCon")
    await conn.subscribe(SUBSCRIPTIONS)
    bench = Bench(
        conn=conn,
        echo_url=BOT_URL,
        captures=REPO / "examples/freeswitch/captures",
        endpoint="sip",
        extension="9998",
    )
    collector = asyncio.create_task(bench.collect_events())
    results = []
    try:
        for chunks in (int(c) for c in args.chunks.split(",")):
            for result in (
                await run_speak_end(bench, args.tone_seconds, chunks),
                await run_interrupt(bench, 6.0, 1.5, chunks),
            ):
                results.append(result)
                print(json.dumps(result), flush=True)
    finally:
        collector.cancel()
        await conn.api("hupall")
        await conn.close()
    out = bench.captures / f"e2e-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"results written to {out}")


if __name__ == "__main__":
    asyncio.run(main())
