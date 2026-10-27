"""Phase-0 bench server for mod_audio_stream (open source edition).

Accepts the module's WebSocket, records what arrives and answers with ``streamAudio`` blocks.
Everything interesting is appended to ``$CAPTURE_DIR/<uuid>.jsonl`` with wall-clock timestamps so the
experiments in docs/specification.md (Phase 0) can be read off afterwards.

Modes (env ``MODE``, or per call via ``POST /config``):

- ``echo``: send the caller's audio back in ``chunk_ms`` blocks.
- ``tone``: on connect, send ``tone_seconds`` of a 440 Hz tone in ``chunk_ms`` blocks, all at once, or paced
  like a real-time pipeline (``paced``, with optional ``jitter_ms`` and ``prebuffer_blocks``), optionally hanging
  up afterwards (``hangup``).

The open source module only writes the file and raises ``mod_audio_stream::play``; nothing plays it.
``run_esl_player`` subscribes to that event and queues ``playback`` on the channel (this is the
piece ``FreeSWITCHControl`` will own in v0.1). Barge-in and hang-up go over the same ESL connection.
"""

import array
import asyncio
import base64
import json
import math
import os
import random
import time

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from esl import EslConnection

ESL_HOST = os.environ.get("FS_ESL_HOST", "127.0.0.1")
ESL_PORT = int(os.environ.get("FS_ESL_PORT", "8021"))
ESL_PASSWORD = os.environ.get("FS_ESL_PASSWORD", "ClueCon")
PORT = int(os.environ.get("ECHO_PORT", "8765"))
OUT_RATE = int(os.environ.get("OUT_SAMPLE_RATE", "16000"))
CAPTURE_DIR = os.environ.get("CAPTURE_DIR", "./captures")
IN_RATE = 16000  # matches "mono 16k" in the dialplan


class CallConfig(BaseModel):
    """Behavior applied to the next WebSocket connections."""

    mode: str = os.environ.get("MODE", "echo")
    chunk_ms: int = int(os.environ.get("CHUNK_MS", "300"))
    tone_seconds: float = float(os.environ.get("TONE_SECONDS", "10"))
    # "broadcast": uuid_broadcast (adds 100 ms of lead frames per file); "sendmsg": ESL execute
    play_method: str = os.environ.get("PLAY_METHOD", "sendmsg")
    # Tone pacing: False sends every block at once; True sends block i when it would be complete in a
    # real-time pipeline (t0 + (i+1) * chunk_ms), plus a random delay in [0, jitter_ms].
    paced: bool = False
    jitter_ms: float = 0.0
    # Hold the first blocks until this many extra blocks are ready (playback safety margin).
    prebuffer_blocks: int = 0
    # Delay before the tone starts, so the caller-side recording is already running.
    start_delay_s: float = 0.0
    # After the last block: "none", "immediate" (uuid_kill right away, like a naive EndFrame handler)
    # or "after_playback" (uuid_kill once the last file has finished playing).
    hangup: str = "none"
    seed: int = 0
    # Sample rate of the generated tone (the module writes .r8 / .r16 / ... accordingly).
    out_rate: int = OUT_RATE
    # "flat", or "ramp": amplitude rises linearly 10%->100% inside each block (shows which frame is lost).
    envelope: str = "flat"


app = FastAPI()
call_config = CallConfig()
background_tasks: set[asyncio.Task] = set()  # asyncio keeps only weak references to tasks
leg_configs: dict[str, CallConfig] = {}  # config in effect for each streaming leg
esl_conn: EslConnection | None = None  # shared by the player and the control endpoints
playbacks_done: dict[str, int] = {}  # finished playback apps per channel
playback_changed = asyncio.Condition()
os.makedirs(CAPTURE_DIR, exist_ok=True)


def spawn(coro) -> asyncio.Task:
    """Start a background task and keep a strong reference until it finishes."""
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    return task


def log_event(uuid: str, kind: str, **fields) -> None:
    """Append one timestamped event to the call's JSONL capture."""
    line = json.dumps({"wall": time.time(), "kind": kind, **fields})
    with open(os.path.join(CAPTURE_DIR, f"{uuid or 'unknown'}.jsonl"), "a") as f:
        f.write(line + "\n")


def stream_audio_message(pcm: bytes, rate: int) -> str:
    """Build the ``streamAudio`` JSON message for raw L16 audio."""
    return json.dumps(
        {
            "type": "streamAudio",
            "data": {
                "audioDataType": "raw",
                "sampleRate": rate,
                "audioData": base64.b64encode(pcm).decode(),
            },
        }
    )


def tone_pcm(seconds: float, rate: int, freq: float = 440.0) -> bytes:
    """Generate a sine tone as L16 mono."""
    n = int(seconds * rate)
    return array.array(
        "h", (int(8000 * math.sin(2 * math.pi * freq * i / rate)) for i in range(n))
    ).tobytes()


@app.post("/config")
async def set_config(config: CallConfig) -> CallConfig:
    """Set mode / chunk size / tone length for the next calls."""
    global call_config
    call_config = config
    return call_config


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    """Handle one mod_audio_stream connection."""
    await ws.accept()
    config = call_config
    uuid = ""
    chunk_bytes = IN_RATE * 2 * config.chunk_ms // 1000
    pending = bytearray()
    frames = 0
    raw = open(os.path.join(CAPTURE_DIR, "last_input.l16"), "wb")
    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                break
            if (text := msg.get("text")) is not None:
                # First text message is the metadata passed as the last uuid_audio_stream argument.
                if not uuid:
                    uuid = text.strip()
                    leg_configs[uuid] = config
                    log_event(uuid, "metadata", text=text, config=config.model_dump())
                    if config.mode == "tone":
                        spawn(send_tone(ws, uuid, config))
                else:
                    log_event(uuid, "text", text=text[:200])
            elif (data := msg.get("bytes")) is not None:
                frames += 1
                raw.write(data)
                log_event(uuid, "audio_in", n=frames, bytes=len(data))
                if config.mode == "echo":
                    pending += data
                    while len(pending) >= chunk_bytes:
                        block, pending = bytes(pending[:chunk_bytes]), pending[chunk_bytes:]
                        await ws.send_text(stream_audio_message(block, IN_RATE))
                        log_event(uuid, "stream_audio_sent", bytes=len(block), rate=IN_RATE)
    except WebSocketDisconnect:
        pass
    finally:
        raw.close()
        leg_configs.pop(uuid, None)
        playbacks_done.pop(uuid, None)
        log_event(uuid, "ws_closed", frames=frames)


async def send_tone(ws: WebSocket, uuid: str, config: CallConfig) -> None:
    """Send the tone in ``chunk_ms`` blocks, all at once or paced in real time; then maybe hang up."""
    await asyncio.sleep(config.start_delay_s)
    pcm = tone_pcm(config.tone_seconds, config.out_rate)
    chunk_bytes = config.out_rate * 2 * config.chunk_ms // 1000
    blocks = [pcm[i : i + chunk_bytes] for i in range(0, len(pcm), chunk_bytes)]
    if config.envelope == "ramp":
        blocks = [apply_ramp(b) for b in blocks]
    rng = random.Random(config.seed)
    t0 = time.monotonic()
    log_event(uuid, "tone_start", blocks=len(blocks), paced=config.paced)
    for i, block in enumerate(blocks):
        if config.paced:
            # Block i is complete at t0 + (i+1) * chunk; with a prebuffer, nothing goes out before
            # block ``prebuffer_blocks`` is complete.
            ready = t0 + (max(i, min(config.prebuffer_blocks, len(blocks) - 1)) + 1) * config.chunk_ms / 1000
            send_at = ready + rng.uniform(0, config.jitter_ms) / 1000
            await asyncio.sleep(max(0.0, send_at - time.monotonic()))
        await ws.send_text(stream_audio_message(block, config.out_rate))
        log_event(uuid, "stream_audio_sent", n=i, bytes=len(block), rate=config.out_rate)
    if config.hangup == "after_playback":
        await wait_playbacks(uuid, len(blocks), timeout=config.tone_seconds + 10)
    if config.hangup in ("immediate", "after_playback"):
        log_event(uuid, "uuid_kill_request", reason=config.hangup)
        log_event(uuid, "uuid_kill", result=await fs_api(f"uuid_kill {uuid}"))


def apply_ramp(block: bytes) -> bytes:
    """Scale an L16 block by a linear 10%->100% ramp."""
    samples = array.array("h", block)
    n = len(samples)
    for i in range(n):
        samples[i] = int(samples[i] * (0.1 + 0.9 * i / max(1, n - 1)))
    return samples.tobytes()


async def wait_playbacks(uuid: str, count: int, timeout: float) -> None:
    """Wait until ``count`` playback apps have finished on the channel (or the timeout expires)."""
    async with playback_changed:
        try:
            await asyncio.wait_for(
                playback_changed.wait_for(lambda: playbacks_done.get(uuid, 0) >= count), timeout
            )
        except TimeoutError:
            log_event(uuid, "wait_playbacks_timeout", done=playbacks_done.get(uuid, 0), expected=count)


async def run_esl_player() -> None:
    """Play every file announced by ``mod_audio_stream::play`` on the streaming leg."""
    global esl_conn
    while True:
        try:
            conn = esl_conn = await EslConnection.connect(ESL_HOST, ESL_PORT, ESL_PASSWORD)
            await conn.subscribe("CHANNEL_EXECUTE_COMPLETE CUSTOM mod_audio_stream::play")
            print("[esl] subscribed to mod_audio_stream::play", flush=True)
            while True:
                event = await conn.next_event()
                uuid = event.get("Unique-ID", "")
                if event.get("Event-Name") == "CHANNEL_EXECUTE_COMPLETE":
                    if event.get("Application") == "playback":
                        async with playback_changed:
                            playbacks_done[uuid] = playbacks_done.get(uuid, 0) + 1
                            playback_changed.notify_all()
                    continue
                if uuid not in leg_configs:
                    continue  # another app's channel (e.g. the Pipecat bot on 9998)
                try:
                    path = json.loads(event.get("_body", ""))["file"]
                except (json.JSONDecodeError, KeyError):
                    continue
                method = leg_configs.get(uuid, call_config).play_method
                log_event(uuid, "play_event", file=path, method=method)
                if method == "sendmsg":
                    result = await conn.execute(uuid, "playback", path)
                else:
                    result = await conn.api(f"uuid_broadcast {uuid} {path} aleg")
                log_event(uuid, "play_queued", file=path, method=method, result=result)
        except (OSError, ConnectionError, asyncio.IncompleteReadError) as exc:
            print(f"[esl] {exc!r}; retrying in 2 s", flush=True)
        esl_conn = None
        await asyncio.sleep(2)


async def fs_api(command: str) -> str:
    """Run a FreeSWITCH API command over the shared ESL connection."""
    if esl_conn is None:
        raise HTTPException(status_code=503, detail="ESL not connected")
    return await esl_conn.api(command)


@app.get("/break/{uuid}")
async def do_break(uuid: str, all: bool = False) -> dict:
    """Experiment: uuid_break with and without ``all`` (does it clear the queued files?)."""
    log_event(uuid, "uuid_break_request", all=all)
    result = await fs_api(f"uuid_break {uuid} all" if all else f"uuid_break {uuid}")
    log_event(uuid, "uuid_break", all=all, result=result)
    return {"result": result}


@app.get("/kill/{uuid}")
async def do_kill(uuid: str) -> dict:
    """Experiment: uuid_kill over ESL."""
    log_event(uuid, "uuid_kill_request")
    result = await fs_api(f"uuid_kill {uuid}")
    log_event(uuid, "uuid_kill", result=result)
    return {"result": result}


@app.on_event("startup")
async def start_esl() -> None:
    """Start the ESL player in the background."""
    spawn(run_esl_player())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
