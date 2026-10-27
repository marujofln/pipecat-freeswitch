"""Phase-0 bench server for mod_audio_stream (open source edition).

Accepts the module's WebSocket, records what arrives and answers with ``streamAudio`` blocks.
Everything interesting is appended to ``$CAPTURE_DIR/<uuid>.jsonl`` with wall-clock timestamps so the
experiments in docs/especificacao.md (Phase 0) can be read off afterwards.

Modes (env ``MODE``, or per call via ``POST /config``):

- ``echo``: send the caller's audio back in ``chunk_ms`` blocks.
- ``tone``: on connect, send ``tone_seconds`` of a 440 Hz tone in ``chunk_ms`` blocks, all at once.

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


app = FastAPI()
call_config = CallConfig()
background_tasks: set[asyncio.Task] = set()  # asyncio keeps only weak references to tasks
leg_configs: dict[str, CallConfig] = {}  # config in effect for each streaming leg
esl_conn: EslConnection | None = None  # shared by the player and the control endpoints
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
        log_event(uuid, "ws_closed", frames=frames)


async def send_tone(ws: WebSocket, uuid: str, config: CallConfig) -> None:
    """Send the whole tone at once in ``chunk_ms`` blocks, to see how the module queues them."""
    pcm = tone_pcm(config.tone_seconds, OUT_RATE)
    chunk_bytes = OUT_RATE * 2 * config.chunk_ms // 1000
    for i in range(0, len(pcm), chunk_bytes):
        block = pcm[i : i + chunk_bytes]
        await ws.send_text(stream_audio_message(block, OUT_RATE))
        log_event(uuid, "stream_audio_sent", bytes=len(block), rate=OUT_RATE)


async def run_esl_player() -> None:
    """Play every file announced by ``mod_audio_stream::play`` on the streaming leg."""
    global esl_conn
    while True:
        try:
            conn = esl_conn = await EslConnection.connect(ESL_HOST, ESL_PORT, ESL_PASSWORD)
            await conn.subscribe("CUSTOM mod_audio_stream::play")
            print("[esl] subscribed to mod_audio_stream::play", flush=True)
            while True:
                event = await conn.events.get()
                uuid = event.get("Unique-ID", "")
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
