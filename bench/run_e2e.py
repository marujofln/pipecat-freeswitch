"""End-to-end check of FreeSWITCHFrameSerializer against the bench FreeSWITCH.

Places SIP/RTP calls to extension 9998, served by ``bench/tone_bot.py`` (a Pipecat pipeline using the
serializer), and checks what the caller hears:

- ``speak_end``: the bot plays a tone and ends the call. Expect the whole tone to be heard and the
  hang-up to come only after it.
- ``interrupt``: the bot is interrupted while the tone plays. Expect the audio to stop at once and not
  resume.
- ``dtmf``: the caller leg presses every keypad key (RFC 2833 over the SIP call). Expect the bot's
  pipeline to receive the same keys as ``InputDTMFFrame``, in order.
- ``auth``: WebSocket handshakes to the bot without the bearer token, or with a wrong one. Expect both
  to be refused (HTTP 403). The calls of the other scenarios carry the right token, set in the
  dialplan with ``STREAM_EXTRA_HEADERS``, so they show it is accepted.

``speak_end`` also measures first-audio latency, FreeSWITCH CPU time for the call, and how clean the tone
sounds: in every 10 ms window of the caller's recording a 440 Hz sine is fitted and the residual reported in dB
below the signal. A click at a block seam shows up as a window with a high residual ("glitch"). It also counts
the playbacks FreeSWITCH ran for the tone.

- ``listen``: the bot plays a tone and keeps the call up. Reports how much of the caller's audio failed to
  reach the bot's pipeline while the tone played: every playback that starts flushes 30-40 ms of it.

``--lookahead off,on`` compares the transport's fixed blocks with the growing blocks of the serializer's
lookahead processor, and ``--tone-speed`` sets how fast the bot produces the tone, as a speech synthesizer would
(a multiple of real time; 0 queues it all at once).

    python3 bench/run_e2e.py                                   # PCMU, 8 kHz stream, 8 kHz blocks
    python3 bench/run_e2e.py --codec OPUS --in-rate 16000 --out-rates 16000,48000

Requires the bench (``docker compose up -d`` in ``examples/freeswitch``) and the bot
(``.venv/bin/python bench/tone_bot.py``). Standard library only.
"""

import argparse
import array
import asyncio
import base64
import json
import math
import statistics
import subprocess
import time
import wave
from pathlib import Path

from esl import EslConnection
from run_experiments import REPO, SILENCE_RMS, SUBSCRIPTIONS, Bench, Call, event_time, ms

BOT_URL = "http://127.0.0.1:8766"
COMPOSE = REPO / "examples/freeswitch/docker-compose.yml"
EXTENSIONS = {8000: "9998", 16000: "9997"}  # stream rate -> dialplan extension
TONE_HZ = 440
GLITCH_DB = -20.0  # a 10 ms window whose residual is above this is counted as a glitch


def freeswitch_cpu_secs() -> float:
    """CPU time (user + system) used so far by the FreeSWITCH process in the bench container."""
    out = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), "exec", "-T", "freeswitch", "cat", "/proc/1/stat"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    fields = out.rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / 100  # utime, stime in clock ticks (USER_HZ = 100)


def tone_residuals(bench: Bench, call: Call) -> list[float]:
    """Residual (dB below the signal) of a fitted 440 Hz sine, per audible 10 ms window.

    Windows next to silence are skipped: the bench's receiving leg drops ~40 ms at the start of a
    talkspurt, and those edges are not seams.
    """
    with wave.open(str(bench.captures / Path(call.recording).name)) as w:
        rate = w.getframerate()
        samples = array.array("h", w.readframes(w.getnframes()))
    n = rate // 100
    sin = [math.sin(2 * math.pi * TONE_HZ * i / rate) for i in range(n)]
    cos = [math.cos(2 * math.pi * TONE_HZ * i / rate) for i in range(n)]
    ss, cc, sc = sum(v * v for v in sin), sum(v * v for v in cos), sum(a * b for a, b in zip(sin, cos))
    det = ss * cc - sc * sc
    windows = []
    for start in range(0, len(samples) - n + 1, n):
        x = samples[start : start + n]
        energy = sum(v * v for v in x)
        if energy / n < 300**2:
            windows.append(None)
            continue
        xs, xc = sum(a * b for a, b in zip(x, sin)), sum(a * b for a, b in zip(x, cos))
        a, b = (xs * cc - xc * sc) / det, (xc * ss - xs * sc) / det
        residual = sum((v - a * s - b * c) ** 2 for v, s, c in zip(x, sin, cos))
        windows.append(10 * math.log10(max(residual, 1e-9) / energy))
    return [
        r
        for i, r in enumerate(windows)
        if r is not None and 0 < i < len(windows) - 1 and windows[i - 1] is not None and windows[i + 1] is not None
    ]


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


def caller_audio_lost_pct(log: list[dict], in_rate: int, start: float, end: float) -> float | None:
    """Share of the caller's tone that did not reach the bot's pipeline between two wall times.

    The caller sends a continuous tone. Audio FreeSWITCH flushes never arrives or arrives as silence,
    so only the frames that carry the tone count as received.
    """
    finished = next((e for e in log if e["kind"] == "finished"), None)
    if not finished or end <= start:
        return None
    received = sum(n for t, n, rms in finished.get("input_arrivals", []) if start <= t < end and rms >= SILENCE_RMS) / (
        2 * in_rate
    )
    return round(max(0.0, 1 - received / (end - start)) * 100, 1)


async def run_speak_end(
    bench: Bench, tone_seconds: float, chunks: int, rates: dict, lookahead: bool, tone_speed: float
) -> dict:
    """Bot plays a tone then queues EndFrame."""
    cpu_before = freeswitch_cpu_secs()
    call = await bench.place_call(
        "bot",
        chunks * 10,
        tone_seconds,
        scenario="speak_end",
        audio_out_10ms_chunks=chunks,
        lookahead=lookahead,
        tone_speed=tone_speed,
        **rates,
    )
    hangup_at = await wait_for_hangup(bench, call.leg_uuid, tone_seconds + 15)
    await bench.hang_up(call)
    cpu = freeswitch_cpu_secs() - cpu_before
    residuals = sorted(tone_residuals(bench, call))
    log = bot_log(bench, call.leg_uuid)
    heard = bench.audible_spans(call)
    heard_s = sum(e - s for s, e in heard)
    hangup = bench.events_for(call.leg_uuid, "CHANNEL_HANGUP_COMPLETE")
    finished = next((e for e in log if e["kind"] == "finished"), {})
    end_queued = first(log, "end_queued")
    tone_queued = first(log, "tone_queued")
    playbacks = [(s, e) for s, e in bench.playbacks(call.leg_uuid) if e is not None]
    return {
        "scenario": "speak_end",
        "codec": bench.codec,
        **rates,
        "block_ms": chunks * 10,
        "lookahead": lookahead,
        "tone_speed": tone_speed,
        "playbacks": len(playbacks),
        "playback_ms": [ms(e - s) for s, e in playbacks],
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
        "first_audio_ms": ms(heard[0][0] - tone_queued) if heard and tone_queued else None,
        "residual_db_median": round(statistics.median(residuals), 1) if residuals else None,
        "residual_db_p99": round(residuals[int(len(residuals) * 0.99) - 1], 1) if residuals else None,
        "glitch_windows": sum(1 for r in residuals if r > GLITCH_DB),
        "windows": len(residuals),
        "freeswitch_cpu_ms": round(cpu * 1000),
    }


async def run_listen(
    bench: Bench, tone_seconds: float, chunks: int, rates: dict, lookahead: bool, tone_speed: float
) -> dict:
    """Bot plays a tone and keeps the call up: how much caller audio is lost while it plays."""
    interrupted_tone, bench.caller_tone = bench.caller_tone, "%(10000,0,1000)"  # no silent half
    try:
        call = await bench.place_call(
            "bot",
            chunks * 10,
            tone_seconds,
            scenario="listen",
            audio_out_10ms_chunks=chunks,
            lookahead=lookahead,
            tone_speed=tone_speed,
            **rates,
        )
    finally:
        bench.caller_tone = interrupted_tone
    await wait_for_hangup(bench, call.leg_uuid, tone_seconds + 15)
    await bench.hang_up(call)
    log = bot_log(bench, call.leg_uuid)
    heard = bench.audible_spans(call)
    heard_s = sum(e - s for s, e in heard)
    tone_queued = first(log, "tone_queued")
    playbacks = [(s, e) for s, e in bench.playbacks(call.leg_uuid) if e is not None]
    return {
        "scenario": "listen",
        "codec": bench.codec,
        **rates,
        "block_ms": chunks * 10,
        "lookahead": lookahead,
        "tone_speed": tone_speed,
        "leg_uuid": call.leg_uuid,
        "tone_s": tone_seconds,
        "playbacks": len(playbacks),
        "playback_ms": [ms(e - s) for s, e in playbacks],
        "caller_audio_lost_pct": caller_audio_lost_pct(log, rates["in_rate"], playbacks[0][0], playbacks[-1][1])
        if playbacks
        else None,
        "heard_s": round(heard_s, 2),
        "holes_heard": len(bench.holes(call)),
        "first_audio_ms": ms(heard[0][0] - tone_queued) if heard and tone_queued else None,
    }


async def handshake_status(path: str, headers: dict[str, str]) -> int:
    """Open a WebSocket handshake to the bot and return the HTTP status of the reply."""
    host, port = BOT_URL.removeprefix("http://").split(":")
    reader, writer = await asyncio.open_connection(host, int(port))
    request = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {base64.b64encode(b'pipecat-freeswit').decode()}",
        "Sec-WebSocket-Version: 13",
        *(f"{key}: {value}" for key, value in headers.items()),
    ]
    writer.write(("\r\n".join(request) + "\r\n\r\n").encode())
    await writer.drain()
    status = await asyncio.wait_for(reader.readline(), 5)
    writer.close()
    return int(status.split()[1])


async def run_auth() -> dict:
    """Handshakes without the token and with a wrong one must be refused."""
    no_token = await handshake_status("/ws", {})
    wrong_token = await handshake_status("/ws", {"Authorization": "Bearer wrong-token"})
    return {
        "scenario": "auth",
        "no_token_status": no_token,
        "wrong_token_status": wrong_token,
        "refused": no_token == 403 and wrong_token == 403,
    }


async def run_interrupt(
    bench: Bench, tone_seconds: float, after_s: float, chunks: int, rates: dict, lookahead: bool, tone_speed: float
) -> dict:
    """Bot is interrupted while playing a tone."""
    call = await bench.place_call(
        "bot",
        chunks * 10,
        tone_seconds,
        scenario="interrupt",
        interrupt_after_s=after_s,
        audio_out_10ms_chunks=chunks,
        lookahead=lookahead,
        tone_speed=tone_speed,
        **rates,
    )
    await wait_for_hangup(bench, call.leg_uuid, after_s + 15)
    await bench.hang_up(call)
    log = bot_log(bench, call.leg_uuid)
    interrupt_at = first(log, "interrupt")
    heard = bench.audible_spans(call)
    after = [(max(s, interrupt_at), e) for s, e in heard if interrupt_at and e > interrupt_at]
    return {
        "scenario": "interrupt",
        "codec": bench.codec,
        **rates,
        "block_ms": chunks * 10,
        "lookahead": lookahead,
        "tone_speed": tone_speed,
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


DTMF_KEYS = "1234567890*#"


async def run_dtmf(bench: Bench, rates: dict) -> dict:
    """The caller presses every key; the bot's pipeline must receive them all, in order."""
    call = await bench.place_call("bot", 40, 6.0, scenario="dtmf", audio_out_10ms_chunks=4, **rates)
    await asyncio.sleep(1.0)  # let the bot's control follow the channel
    sent_at = time.time()
    await bench.conn.api(f"uuid_send_dtmf {call.caller_uuid} {DTMF_KEYS}")
    await wait_for_hangup(bench, call.leg_uuid, 15)
    await bench.hang_up(call)
    keys = [e for e in bot_log(bench, call.leg_uuid) if e["kind"] == "dtmf"]
    return {
        "scenario": "dtmf",
        "codec": bench.codec,
        **rates,
        "leg_uuid": call.leg_uuid,
        "sent": DTMF_KEYS,
        "received": "".join(e["digit"] for e in keys),
        "send_to_last_key_ms": ms(keys[-1]["wall"] - sent_at) if keys else None,
    }


async def main():
    """Run the scenarios and print the results."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--chunks", default="4,20", help="audio_out_10ms_chunks values (block = 10 ms each)")
    parser.add_argument("--tone-seconds", type=float, default=3.0)
    parser.add_argument("--codec", default="PCMU", help="absolute_codec_string for the calls (PCMU, OPUS, ...)")
    parser.add_argument("--in-rate", type=int, default=8000, choices=sorted(EXTENSIONS))
    parser.add_argument("--out-rates", default="8000", help="freeswitch_out_sample_rate values to compare")
    parser.add_argument("--pipeline-out-rate", type=int, default=24000, help="pipeline (TTS) output rate")
    parser.add_argument("--scenarios", default="auth,speak_end,listen,interrupt,dtmf")
    parser.add_argument(
        "--lookahead", default="off", help="lookahead processor in the bot's pipeline: on, off or off,on"
    )
    parser.add_argument(
        "--tone-speed",
        type=float,
        default=0.0,
        help="how fast the bot produces the tone, as a multiple of real time (0: all at once)",
    )
    args = parser.parse_args()

    conn = await EslConnection.connect("127.0.0.1", 8021, "ClueCon")
    await conn.subscribe(SUBSCRIPTIONS)
    bench = Bench(
        conn=conn,
        echo_url=BOT_URL,
        captures=REPO / "examples/freeswitch/captures",
        endpoint="sip",
        extension=EXTENSIONS[args.in_rate],
        codec=args.codec,
    )
    collector = asyncio.create_task(bench.collect_events())
    results = []
    try:
        scenarios = args.scenarios.split(",")
        if "auth" in scenarios:
            result = await run_auth()
            results.append(result)
            print(json.dumps(result), flush=True)
        for out_rate in (int(r) for r in args.out_rates.split(",")):
            rates = {
                "in_rate": args.in_rate,
                "out_rate": out_rate,
                "pipeline_out_rate": args.pipeline_out_rate,
            }
            if "dtmf" in scenarios:
                result = await run_dtmf(bench, rates)
                results.append(result)
                print(json.dumps(result), flush=True)
            for chunks in (int(c) for c in args.chunks.split(",")):
                for lookahead in (value == "on" for value in args.lookahead.split(",")):
                    for name, run in (("speak_end", run_speak_end), ("listen", run_listen)):
                        if name in scenarios:
                            result = await run(bench, args.tone_seconds, chunks, rates, lookahead, args.tone_speed)
                            results.append(result)
                            print(json.dumps(result), flush=True)
                    if "interrupt" in scenarios:
                        result = await run_interrupt(bench, 6.0, 1.5, chunks, rates, lookahead, args.tone_speed)
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
