"""Compare the RTP that FreeSWITCH puts on the wire with what the caller leg hears.

Places one SIP/RTP bench call (see ``run_experiments.py``) while ``tcpdump`` captures the RTP on ``lo``, then
decodes the PCMU sent by the streaming leg and prints its 10 ms RMS envelope next to the caller's recording.
The tone uses a ramp envelope per block (10% -> 100%), so a missing frame shows up as a step in the ramp.

    python3 bench/rtp_wire.py                 # 1 s tone, 200 ms blocks, sent at once
    python3 bench/rtp_wire.py --paced         # blocks sent in real time

Requires the bench running and the capture image built
(``docker compose --profile tools build tcpdump`` in ``examples/freeswitch``). Standard library only.
"""

import argparse
import array
import asyncio
import math
import struct
import subprocess
import sys
import time
import wave
from pathlib import Path

from esl import EslConnection
from run_experiments import REPO, SUBSCRIPTIONS, Bench

COMPOSE = REPO / "examples/freeswitch/docker-compose.yml"
CAPTURE_CONTAINER = "pipecat-freeswitch-tcpdump"
RTP_PORTS = "16384-16484"  # switch.conf.xml rtp-start-port / rtp-end-port
WINDOW_S = 0.010
SILENCE_RMS = 100


def ulaw_to_linear(u: int) -> int:
    """Decode one G.711 mu-law byte."""
    u = ~u & 0xFF
    magnitude = (((u & 0x0F) << 3) + 0x84) << ((u >> 4) & 0x07)
    return -(magnitude - 0x84) if u & 0x80 else magnitude - 0x84


ULAW = [ulaw_to_linear(i) for i in range(256)]


def read_udp_packets(path: Path):
    """Yield (time, src_port, dst_port, payload) for IPv4/UDP packets in a pcap file."""
    data = path.read_bytes()
    magic = struct.unpack("<I", data[:4])[0]
    if magic in (0xA1B2C3D4, 0xA1B23C4D):
        endian = "<"
    elif magic in (0xD4C3B2A1, 0x4D3CB2A1):
        endian = ">"
    else:
        raise ValueError("not a pcap file")
    nanos = magic in (0xA1B23C4D, 0x4D3CB2A1)
    linktype = struct.unpack(endian + "I", data[20:24])[0]
    link_header = {0: 4, 1: 14, 113: 16, 276: 20}[linktype]
    offset = 24
    while offset + 16 <= len(data):
        sec, frac, caplen, _ = struct.unpack(endian + "IIII", data[offset : offset + 16])
        frame = data[offset + 16 : offset + 16 + caplen]
        offset += 16 + caplen
        ip = frame[link_header:]
        if len(ip) < 20 or ip[0] >> 4 != 4 or ip[9] != 17:
            continue
        udp = ip[(ip[0] & 0x0F) * 4 :]
        src, dst = struct.unpack("!HH", udp[:4])
        yield sec + frac / (1e9 if nanos else 1e6), src, dst, udp[8:]


def decode_rtp_flow(path: Path, src_port: int) -> dict:
    """Decode the PCMU RTP stream sent from ``src_port``: audio laid out by RTP timestamp, plus stats."""
    packets = []
    for t, src, _, payload in read_udp_packets(path):
        if src != src_port or len(payload) < 12 or payload[0] >> 6 != 2:
            continue
        cc = payload[0] & 0x0F
        pt = payload[1] & 0x7F
        marker = payload[1] >> 7
        seq, ts = struct.unpack("!HI", payload[2:8])
        body = payload[12 + 4 * cc :]
        if payload[0] & 0x10:  # header extension
            ext_len = struct.unpack("!H", body[2:4])[0]
            body = body[4 + 4 * ext_len :]
        packets.append({"t": t, "seq": seq, "ts": ts, "pt": pt, "marker": marker, "body": body})
    audio = [p for p in packets if p["pt"] == 0]
    if not audio:
        return {"packets": len(packets), "audio_packets": 0}
    base = audio[0]["ts"]
    end = max(p["ts"] - base + len(p["body"]) for p in audio)
    pcm = array.array("h", [0]) * end
    for p in audio:
        start = p["ts"] - base
        pcm[start : start + len(p["body"])] = array.array("h", (ULAW[b] for b in p["body"]))
    seq_gaps = sum(1 for a, b in zip(audio, audio[1:]) if (b["seq"] - a["seq"]) % 65536 != 1)
    ts_jumps = sum(1 for a, b in zip(audio, audio[1:]) if b["ts"] - a["ts"] != len(a["body"]))
    return {
        "packets": len(packets),
        "audio_packets": len(audio),
        "other_payload_types": sorted({p["pt"] for p in packets if p["pt"] != 0}),
        "markers": sum(p["marker"] for p in audio),
        "seq_gaps": seq_gaps,
        "timestamp_jumps": ts_jumps,
        "pcm": pcm,
    }


def rms_envelope(samples: array.array, rate: int) -> list[int]:
    """10 ms RMS values, starting at the first non-silent window."""
    window = int(rate * WINDOW_S)
    values = [
        round(math.sqrt(sum(v * v for v in samples[i : i + window]) / window))
        for i in range(0, len(samples) - window + 1, window)
    ]
    first = next((i for i, v in enumerate(values) if v >= SILENCE_RMS), len(values))
    last = max((i for i, v in enumerate(values) if v >= SILENCE_RMS), default=-1)
    return values[first : last + 1]


def silent_windows(envelope: list[int]) -> int:
    """Number of silent 10 ms windows inside the audible part."""
    return sum(1 for v in envelope if v < SILENCE_RMS)


def compose(*args: str) -> list[str]:
    """Docker compose command line for the bench."""
    return ["docker", "compose", "-f", str(COMPOSE), "--profile", "tools", *args]


async def main() -> None:
    """Capture one call and compare wire audio with the caller's recording."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--chunk-ms", type=int, default=200)
    parser.add_argument("--tone-seconds", type=float, default=1.0)
    parser.add_argument("--paced", action="store_true")
    parser.add_argument("--play-method", default="sendmsg", choices=["broadcast", "sendmsg"])
    args = parser.parse_args()

    captures = REPO / "examples/freeswitch/captures"
    name = f"wire-{time.strftime('%Y%m%d-%H%M%S')}"
    subprocess.run(["docker", "rm", "-f", CAPTURE_CONTAINER], capture_output=True)
    subprocess.run(
        compose(
            "run",
            "-d",
            "--rm",
            "--name",
            CAPTURE_CONTAINER,
            "tcpdump",
            "-i",
            "lo",
            "-Z",
            "root",
            "-w",
            f"/captures/{name}.pcap",
            "udp",
            "portrange",
            RTP_PORTS,
        ),
        check=True,
        capture_output=True,
    )
    await asyncio.sleep(2.0)  # let tcpdump start

    conn = await EslConnection.connect("127.0.0.1", 8021, "ClueCon")
    await conn.subscribe(SUBSCRIPTIONS)
    bench = Bench(
        conn=conn,
        echo_url="http://127.0.0.1:8765",
        captures=captures,
        play_method=args.play_method,
        out_rate=8000,
        endpoint="sip",
    )
    collector = asyncio.create_task(bench.collect_events())
    try:
        call = await bench.place_call(
            "tone",
            args.chunk_ms,
            args.tone_seconds,
            envelope="ramp",
            paced=args.paced,
            start_delay_s=0.5,
        )
        port = int(await conn.api(f"uuid_getvar {call.leg_uuid} local_media_port"))
        await asyncio.sleep(0.5 + args.tone_seconds + 2.0)
        await bench.hang_up(call)
    finally:
        collector.cancel()
        await conn.close()
        subprocess.run(["docker", "stop", CAPTURE_CONTAINER], capture_output=True)

    pcap = captures / f"{name}.pcap"
    flow = decode_rtp_flow(pcap, port)
    if not flow.get("audio_packets"):
        sys.exit(f"no PCMU packets from port {port} in {pcap}")
    wire = rms_envelope(flow.pop("pcm"), 8000)
    with wave.open(str(captures / Path(call.recording).name)) as w:
        heard_samples = array.array("h", w.readframes(w.getnframes()))
        heard = rms_envelope(heard_samples, w.getframerate())

    print(f"pcap: {pcap}")
    print(f"streaming leg RTP from port {port}: {flow}")
    print(f"wire  ({silent_windows(wire)} silent 10 ms windows inside the audio): {wire}")
    print(f"heard ({silent_windows(heard)} silent 10 ms windows inside the audio): {heard}")


if __name__ == "__main__":
    asyncio.run(main())
