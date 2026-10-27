"""Phase-0 experiment runner: places loopback calls to extension 9999 and measures mod_audio_stream.

Requires the bench from ``examples/freeswitch`` to be running (``docker compose up -d``).
Standard library only::

    python3 bench/run_experiments.py                     # all experiments
    python3 bench/run_experiments.py -e latency --chunks 100,500

Experiments:

- ``input``:   L16 arrival from FreeSWITCH (frame size, inter-arrival jitter).
- ``latency``: ``streamAudio`` block size vs time to first audio, file count and gaps between files.
- ``break``:   ``uuid_break`` with and without ``all`` while several files are queued.
- ``kill``:    ``uuid_kill`` over ESL, time until hang-up and WebSocket close.
- ``pacing``:  blocks sent in real time (as a paced pipeline would), with jitter and an optional prebuffer:
  does the playback queue run dry between blocks?
- ``hangup``:  ``uuid_kill`` right after the last block vs after the last file finished playing:
  how much of the end of the speech is cut off?

Each call (``--endpoint sip``, the default: a real SIP/RTP call from FreeSWITCH to itself through the
bench-only profile on 127.0.0.1:5090; or ``--endpoint loopback``): the caller leg plays a 1 kHz tone and records what it hears
(``captures/<caller>-caller.wav``); the dialplan leg streams to the echo server, which logs to
``captures/<leg>.jsonl``. Results are printed and written to ``captures/results-<timestamp>.json``.
"""

import argparse
import array
import asyncio
import json
import math
import statistics
import time
import urllib.request
import uuid as uuidlib
import wave
from dataclasses import dataclass, field
from pathlib import Path

from esl import EslConnection

REPO = Path(__file__).resolve().parent.parent
SUBSCRIPTIONS = (
    "CHANNEL_EXECUTE CHANNEL_EXECUTE_COMPLETE CHANNEL_HANGUP_COMPLETE "
    "CUSTOM mod_audio_stream::connect mod_audio_stream::disconnect "
    "mod_audio_stream::error mod_audio_stream::play"
)
SILENCE_RMS = 300  # 10 ms windows below this are treated as silence
WINDOW_S = 0.010
# loopback: no RTP, but mod_loopback drops queued frames on every playback start (audio sync).
# sip: real SIP/RTP (PCMU) through the bench-only profile on 127.0.0.1:5090.
DIAL_STRINGS = {
    "loopback": "loopback/{extension}/default",
    "sip": "sofia/bench/{extension}@127.0.0.1:5090",
}


def event_time(event: dict) -> float:
    """Host time (epoch seconds) at which the event arrived; see ``Bench.collect_events``."""
    return event["_t"]


@dataclass
class Call:
    """One bench call: the caller leg and the dialplan leg that streams to the echo server."""

    caller_uuid: str
    leg_uuid: str = ""
    record_start: float = 0.0
    recording: str = ""


@dataclass
class Bench:
    """Shared state: ESL connection, collected events and paths."""

    conn: EslConnection
    echo_url: str
    captures: Path
    play_method: str = "sendmsg"
    out_rate: int = 16000
    endpoint: str = "loopback"
    extension: str = "9999"
    events: list[dict] = field(default_factory=list)

    async def collect_events(self) -> None:
        """Keep every event, stamped with the host time it arrived.

        FreeSWITCH's ``Event-Date-Timestamp`` comes from a cached clock that lags the host clock,
        so all timings use the arrival time instead, on the same clock as the echo server's logs.
        """
        while True:
            event = await self.conn.next_event()
            event["_t"] = time.time()
            self.events.append(event)

    def events_for(self, uuid: str, name: str, since: float = 0.0) -> list[dict]:
        """Return events of one channel by Event-Name (or Event-Subclass), in order."""
        return [
            e
            for e in self.events
            if e.get("Unique-ID") == uuid
            and name in (e.get("Event-Name"), e.get("Event-Subclass"))
            and event_time(e) >= since
        ]

    def playbacks(self, uuid: str) -> list[tuple[float, float | None]]:
        """Return (start, end) of each playback on a channel; end is None if it never finished."""
        starts = [
            event_time(e)
            for e in self.events_for(uuid, "CHANNEL_EXECUTE")
            if e.get("Application") == "playback"
        ]
        ends = [
            event_time(e)
            for e in self.events_for(uuid, "CHANNEL_EXECUTE_COMPLETE")
            if e.get("Application") == "playback"
        ]
        return [(s, ends[i] if i < len(ends) else None) for i, s in enumerate(starts)]

    def capture(self, uuid: str) -> list[dict]:
        """Read the echo server's JSONL log for a dialplan leg."""
        path = self.captures / f"{uuid}.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

    def http(self, method: str, path: str, body: dict | None = None) -> dict:
        """Call the echo server's HTTP API (blocking; run it in a thread)."""
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{self.echo_url}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())

    async def echo_api(self, method: str, path: str, body: dict | None = None) -> dict:
        """Async wrapper around ``http``."""
        return await asyncio.to_thread(self.http, method, path, body)

    async def place_call(
        self, mode: str, chunk_ms: int, tone_seconds: float = 3.0, **config: object
    ) -> Call:
        """Configure the echo server, originate a loopback call to 9999 and start recording.

        Extra keyword arguments are passed to the echo server's ``/config`` (``paced``, ``jitter_ms``, ...).
        """
        await self.echo_api("POST", "/config", {
            "mode": mode, "chunk_ms": chunk_ms, "tone_seconds": tone_seconds,
            "play_method": self.play_method, "out_rate": self.out_rate, **config,
        })
        call = Call(caller_uuid=str(uuidlib.uuid4()))
        since = time.time()
        await self.conn.api(
            f"originate {{origination_uuid={call.caller_uuid},RECORD_READ_ONLY=true}}"
            f"{DIAL_STRINGS[self.endpoint].format(extension=self.extension)} "
            "&endless_playback(tone_stream://%(500,500,1000))"
        )
        call.leg_uuid = await self.wait_for_connect(since)
        call.recording = f"/captures/{call.caller_uuid}-caller.wav"
        call.record_start = time.time()
        await self.conn.api(f"uuid_record {call.caller_uuid} start {call.recording}")
        return call

    async def wait_for_connect(self, since: float, timeout: float = 10.0) -> str:
        """Wait for mod_audio_stream::connect and return the streaming leg's UUID."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            for e in self.events:
                if e.get("Event-Subclass") == "mod_audio_stream::connect" and event_time(e) >= since:
                    return e["Unique-ID"]
            await asyncio.sleep(0.05)
        raise TimeoutError("no mod_audio_stream::connect; is the echo server running?")

    async def wait_first_playback(self, call: Call, timeout: float = 10.0) -> float:
        """Wait until the dialplan leg starts its first playback and return its time."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pbs := self.playbacks(call.leg_uuid):
                return pbs[0][0]
            await asyncio.sleep(0.05)
        raise TimeoutError("no playback started on the streaming leg")

    async def hang_up(self, call: Call) -> None:
        """Stop recording and hang up both legs; give events time to arrive."""
        await self.conn.api(f"uuid_record {call.caller_uuid} stop all")
        await self.conn.api(f"uuid_kill {call.caller_uuid}")
        await asyncio.sleep(1.0)

    def audible_spans(self, call: Call) -> list[tuple[float, float]]:
        """Return (start, end) wall times of non-silent audio heard by the caller."""
        path = self.captures / Path(call.recording).name
        if not path.exists():
            return []
        with wave.open(str(path)) as w:
            rate, width, channels = w.getframerate(), w.getsampwidth(), w.getnchannels()
            samples = array.array("h", w.readframes(w.getnframes()))
        if width != 2:
            raise ValueError(f"unexpected sample width {width}")
        window = int(rate * channels * WINDOW_S)
        spans: list[tuple[float, float]] = []
        for i in range(0, len(samples) - window + 1, window):
            chunk = samples[i : i + window]
            rms = math.sqrt(sum(s * s for s in chunk) / len(chunk))
            t = call.record_start + i / (rate * channels)
            if rms >= SILENCE_RMS:
                # Merge only adjacent windows: a single silent window is a real hole.
                if spans and t - spans[-1][1] < WINDOW_S / 2:
                    spans[-1] = (spans[-1][0], t + WINDOW_S)
                else:
                    spans.append((t, t + WINDOW_S))
        return spans

    def holes(self, call: Call) -> list[float]:
        """Durations (s) of the silent holes between audible spans heard by the caller."""
        spans = self.audible_spans(call)
        return [b[0] - a[1] for a, b in zip(spans, spans[1:])]


def ms(seconds: float | None) -> float | None:
    """Convert seconds to rounded milliseconds."""
    return None if seconds is None else round(seconds * 1000, 1)


def describe(values: list[float]) -> dict:
    """Summary statistics in milliseconds."""
    if not values:
        return {}
    ordered = sorted(values)
    return {
        "n": len(values),
        "mean_ms": ms(statistics.mean(values)),
        "p50_ms": ms(ordered[len(ordered) // 2]),
        "p95_ms": ms(ordered[int(len(ordered) * 0.95) - 1]),
        "max_ms": ms(ordered[-1]),
    }


async def experiment_input(bench: Bench, seconds: float = 5.0) -> dict:
    """L16 arrival: frame sizes and inter-arrival times at the echo server."""
    call = await bench.place_call("echo", 300)
    await asyncio.sleep(seconds)
    await bench.hang_up(call)
    frames = [e for e in bench.capture(call.leg_uuid) if e["kind"] == "audio_in"]
    walls = [e["wall"] for e in frames]
    gaps = [b - a for a, b in zip(walls, walls[1:])]
    sizes = sorted({e["bytes"] for e in frames})
    span = walls[-1] - walls[0] if len(walls) > 1 else 0.0
    return {
        "leg_uuid": call.leg_uuid,
        "frames": len(frames),
        "frame_bytes": sizes,
        "frame_ms_at_16k": [b / 32 for b in sizes],
        "arrival_span_s": round(span, 3),
        "inter_arrival": describe(gaps),
    }


async def experiment_latency(bench: Bench, chunk_ms: int, tone_seconds: float = 3.0) -> dict:
    """One block size: time to first audio, number of files, gaps between consecutive files."""
    call = await bench.place_call("tone", chunk_ms, tone_seconds)
    await bench.wait_first_playback(call)
    await asyncio.sleep(tone_seconds + 2.0)
    await bench.hang_up(call)
    log = bench.capture(call.leg_uuid)
    sent = [e["wall"] for e in log if e["kind"] == "stream_audio_sent"]
    play_events = [event_time(e) for e in bench.events_for(call.leg_uuid, "mod_audio_stream::play")]
    pbs = bench.playbacks(call.leg_uuid)
    finished = [(s, e) for s, e in pbs if e is not None]
    gaps = [pbs[i + 1][0] - finished[i][1] for i in range(min(len(finished), len(pbs) - 1))]
    heard = bench.audible_spans(call)
    heard_total = sum(e - s for s, e in heard)
    return {
        "play_method": bench.play_method,
        "chunk_ms": chunk_ms,
        "leg_uuid": call.leg_uuid,
        "blocks_sent": len(sent),
        "play_events": len(play_events),
        "playbacks_started": len(pbs),
        "playbacks_finished": len(finished),
        "send_to_play_event_ms": ms(play_events[0] - sent[0]) if sent and play_events else None,
        "send_to_first_playback_ms": ms(pbs[0][0] - sent[0]) if sent and pbs else None,
        "playback_span_s": round(finished[-1][1] - pbs[0][0], 3) if finished else None,
        "tone_seconds": tone_seconds,
        "gap_between_files": describe(gaps),
        "heard_s": round(heard_total, 2),
        "heard_segments": len(heard),
        "holes_heard": len(bench.holes(call)),
        "holes_heard_ms": ms(sum(bench.holes(call))),
    }


async def experiment_break(bench: Bench, use_all: bool, tone_seconds: float = 8.0) -> dict:
    """uuid_break 2 s into an 8 s tone sent as 500 ms files: does the queue keep playing?"""
    call = await bench.place_call("tone", 500, tone_seconds)
    first = await bench.wait_first_playback(call)
    await asyncio.sleep(max(0.0, first + 2.0 - time.time()))
    break_at = time.time()
    result = await bench.echo_api("GET", f"/break/{call.leg_uuid}?all={'true' if use_all else 'false'}")
    await asyncio.sleep(4.0)
    await bench.hang_up(call)
    pbs = bench.playbacks(call.leg_uuid)
    heard_after = [(max(s, break_at), e) for s, e in bench.audible_spans(call) if e > break_at]
    return {
        "play_method": bench.play_method,
        "all": use_all,
        "leg_uuid": call.leg_uuid,
        "control_result": result.get("result"),
        "playbacks_before_break": sum(1 for s, _ in pbs if s < break_at),
        "playbacks_started_after_break": sum(1 for s, _ in pbs if s >= break_at),
        "heard_after_break_s": round(sum(e - s for s, e in heard_after), 2),
        "last_audio_after_break_ms": ms(heard_after[-1][1] - break_at) if heard_after else 0.0,
    }


async def experiment_kill(bench: Bench) -> dict:
    """uuid_kill over ESL: time until hang-up, module disconnect and WebSocket close."""
    call = await bench.place_call("echo", 300)
    await asyncio.sleep(2.0)
    kill_at = time.time()
    result = await bench.echo_api("GET", f"/kill/{call.leg_uuid}")
    await asyncio.sleep(2.0)
    hangups = bench.events_for(call.leg_uuid, "CHANNEL_HANGUP_COMPLETE", kill_at)
    disconnects = bench.events_for(call.leg_uuid, "mod_audio_stream::disconnect", kill_at)
    closed = [e["wall"] for e in bench.capture(call.leg_uuid) if e["kind"] == "ws_closed"]
    caller_hangups = bench.events_for(call.caller_uuid, "CHANNEL_HANGUP_COMPLETE", kill_at)
    await bench.conn.api("hupall")
    return {
        "leg_uuid": call.leg_uuid,
        "control_result": result.get("result"),
        "kill_to_hangup_ms": ms(event_time(hangups[0]) - kill_at) if hangups else None,
        "hangup_cause": hangups[0].get("Hangup-Cause") if hangups else None,
        "kill_to_module_disconnect_ms": ms(event_time(disconnects[0]) - kill_at) if disconnects else None,
        "kill_to_ws_closed_ms": ms(closed[0] - kill_at) if closed else None,
        "caller_hung_up": bool(caller_hangups),
    }


def tone_start(log: list[dict]) -> float | None:
    """Wall time at which the echo server started producing the tone."""
    return next((e["wall"] for e in log if e["kind"] == "tone_start"), None)


async def experiment_pacing(
    bench: Bench, chunk_ms: int, jitter_ms: float, prebuffer: int, tone_seconds: float = 3.0
) -> dict:
    """Real-time paced blocks: count playback underruns (queue empty when a file ends)."""
    call = await bench.place_call(
        "tone", chunk_ms, tone_seconds, paced=True, jitter_ms=jitter_ms, prebuffer_blocks=prebuffer,
        start_delay_s=0.5, seed=chunk_ms * 1000 + int(jitter_ms),
    )
    await asyncio.sleep(0.5 + tone_seconds + (prebuffer * chunk_ms + jitter_ms) / 1000 + 1.5)
    await bench.hang_up(call)
    log = bench.capture(call.leg_uuid)
    pbs = bench.playbacks(call.leg_uuid)
    finished = [(s, e) for s, e in pbs if e is not None]
    gaps = [pbs[i + 1][0] - finished[i][1] for i in range(min(len(finished), len(pbs) - 1))]
    underruns = [g for g in gaps if g > 0.010]
    heard = bench.audible_spans(call)
    start = tone_start(log)
    return {
        "chunk_ms": chunk_ms,
        "jitter_ms": jitter_ms,
        "prebuffer_blocks": prebuffer,
        "leg_uuid": call.leg_uuid,
        "blocks": sum(1 for e in log if e["kind"] == "stream_audio_sent"),
        "playbacks": len(pbs),
        "underruns": len(underruns),
        "underrun_silence_ms": ms(sum(underruns)),
        "max_underrun_ms": ms(max(underruns)) if underruns else 0.0,
        "first_audio_ms": ms(pbs[0][0] - start) if pbs and start else None,
        "heard_segments": len(heard),
        "heard_s": round(sum(e - s for s, e in heard), 2),
        "holes_heard": len(bench.holes(call)),
        "holes_heard_ms": ms(sum(bench.holes(call))),
    }


async def experiment_hangup(bench: Bench, mode: str, chunk_ms: int = 200, tone_seconds: float = 3.0) -> dict:
    """Paced tone, then uuid_kill right after the last block or after the last file finished."""
    call = await bench.place_call(
        "tone", chunk_ms, tone_seconds, paced=True, start_delay_s=0.5, hangup=mode,
    )
    await asyncio.sleep(0.5 + tone_seconds + 3.0)
    await bench.hang_up(call)
    log = bench.capture(call.leg_uuid)
    sent = [e["wall"] for e in log if e["kind"] == "stream_audio_sent"]
    pbs = bench.playbacks(call.leg_uuid)
    hangups = bench.events_for(call.leg_uuid, "CHANNEL_HANGUP_COMPLETE")
    heard = bench.audible_spans(call)
    heard_s = sum(e - s for s, e in heard)
    return {
        "hangup": mode,
        "chunk_ms": chunk_ms,
        "leg_uuid": call.leg_uuid,
        "blocks": len(sent),
        "playbacks_started": len(pbs),
        "playbacks_finished": sum(1 for _, e in pbs if e is not None),
        "tone_s": tone_seconds,
        "heard_s": round(heard_s, 2),
        "cut_off_ms": ms(max(0.0, tone_seconds - heard_s)),
        "last_block_to_hangup_ms": ms(event_time(hangups[0]) - sent[-1]) if hangups and sent else None,
    }


async def main() -> None:
    """Parse arguments, run the selected experiments and save the results."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-e", "--experiments", default="input,latency,break,kill")
    parser.add_argument("--chunks", default="100,200,300,500,1000", help="block sizes (ms) for latency")
    parser.add_argument("--pacing-chunks", default="40,100,200", help="block sizes (ms) for pacing")
    parser.add_argument("--jitters", default="0,20,50", help="max random send delay (ms) for pacing")
    parser.add_argument("--prebuffers", default="0,1", help="prebuffer sizes (blocks) for pacing")
    parser.add_argument(
        "--play-method", default="sendmsg", choices=["broadcast", "sendmsg"],
        help="how the echo server plays files: uuid_broadcast or ESL sendmsg execute",
    )
    parser.add_argument("--out-rate", type=int, default=16000, help="tone sample rate sent in streamAudio")
    parser.add_argument(
        "--endpoint", default="sip", choices=sorted(DIAL_STRINGS),
        help="how calls reach extension 9999: SIP/RTP to itself, or mod_loopback",
    )
    parser.add_argument("--esl", default="127.0.0.1:8021")
    parser.add_argument("--esl-password", default="ClueCon")
    parser.add_argument("--echo-url", default="http://127.0.0.1:8765")
    parser.add_argument("--captures", type=Path, default=REPO / "examples/freeswitch/captures")
    args = parser.parse_args()

    host, port = args.esl.split(":")
    conn = await EslConnection.connect(host, int(port), args.esl_password)
    await conn.subscribe(SUBSCRIPTIONS)
    bench = Bench(
        conn=conn, echo_url=args.echo_url, captures=args.captures, play_method=args.play_method,
        out_rate=args.out_rate, endpoint=args.endpoint,
    )
    collector = asyncio.create_task(bench.collect_events())

    selected = args.experiments.split(",")
    results: dict[str, object] = {
        "started": time.strftime("%Y-%m-%dT%H:%M:%S"), "play_method": args.play_method,
        "out_rate": args.out_rate, "endpoint": args.endpoint,
    }
    try:
        if "input" in selected:
            print("== input: L16 arrival", flush=True)
            results["input"] = await experiment_input(bench)
            print(json.dumps(results["input"], indent=2), flush=True)
        if "latency" in selected:
            results["latency"] = []
            for chunk in (int(c) for c in args.chunks.split(",")):
                print(f"== latency: chunk_ms={chunk}", flush=True)
                r = await experiment_latency(bench, chunk)
                results["latency"].append(r)
                print(json.dumps(r, indent=2), flush=True)
        if "break" in selected:
            results["break"] = []
            for use_all in (False, True):
                print(f"== break: all={use_all}", flush=True)
                r = await experiment_break(bench, use_all)
                results["break"].append(r)
                print(json.dumps(r, indent=2), flush=True)
        if "kill" in selected:
            print("== kill", flush=True)
            results["kill"] = await experiment_kill(bench)
            print(json.dumps(results["kill"], indent=2), flush=True)
        if "pacing" in selected:
            results["pacing"] = []
            for chunk in (int(c) for c in args.pacing_chunks.split(",")):
                for jitter in (float(j) for j in args.jitters.split(",")):
                    for prebuffer in (int(p) for p in args.prebuffers.split(",")):
                        print(f"== pacing: chunk_ms={chunk} jitter_ms={jitter} prebuffer={prebuffer}", flush=True)
                        r = await experiment_pacing(bench, chunk, jitter, prebuffer)
                        results["pacing"].append(r)
                        print(json.dumps(r), flush=True)
        if "hangup" in selected:
            results["hangup"] = []
            for mode in ("immediate", "after_playback"):
                print(f"== hangup: {mode}", flush=True)
                r = await experiment_hangup(bench, mode)
                results["hangup"].append(r)
                print(json.dumps(r, indent=2), flush=True)
    finally:
        collector.cancel()
        await conn.api("hupall")
        await conn.close()

    out = args.captures / f"results-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"results written to {out}")


if __name__ == "__main__":
    asyncio.run(main())
