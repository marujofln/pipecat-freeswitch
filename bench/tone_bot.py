"""End-to-end test bot: a real Pipecat pipeline with FreeSWITCHFrameSerializer, no AI services.

Dial extension 9998 (``uuid_audio_stream ... ws://127.0.0.1:8766/ws mono 8k ${uuid}``) or 9997 (``16k``). The bot
counts the caller audio it receives and plays a tone, then, depending on the scenario set with
``POST /config``:

- ``speak_end``: queue ``EndFrame`` right after the tone (the serializer must let it finish, then hang up).
- ``interrupt``: push an interruption ``interrupt_after_s`` after the tone starts (playback must stop
  at once and stay stopped), then end the call.

Events go to ``$CAPTURE_DIR/<uuid>-bot.jsonl``. Run from the repository's venv:
``.venv/bin/python bench/tone_bot.py``; ``bench/run_e2e.py`` drives the calls.
"""

import array
import asyncio
import json
import math
import os
import time

import uvicorn
from fastapi import FastAPI, WebSocket
from pipecat.frames.frames import (
    EndFrame,
    Frame,
    InputAudioRawFrame,
    InterruptionWorkerFrame,
    OutputAudioRawFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport
from pipecat.workers.runner import WorkerRunner
from pydantic import BaseModel

from pipecat_freeswitch import FreeSWITCHFrameSerializer

PORT = int(os.environ.get("BOT_PORT", "8766"))
CAPTURE_DIR = os.environ.get(
    "CAPTURE_DIR", os.path.join(os.path.dirname(__file__), "..", "examples/freeswitch/captures")
)


class BotConfig(BaseModel):
    """Scenario for the next calls."""

    scenario: str = "speak_end"
    tone_seconds: float = 3.0
    interrupt_after_s: float = 1.5
    audio_out_10ms_chunks: int = 4
    # Stream rate given to uuid_audio_stream: 8000 for extension 9998, 16000 for 9997.
    in_rate: int = 8000
    # Rate of the streamAudio blocks sent to FreeSWITCH (freeswitch_out_sample_rate).
    out_rate: int = 8000
    # Pipeline output rate (the "TTS" rate); the serializer resamples it to out_rate.
    pipeline_out_rate: int = 8000


app = FastAPI()
config = BotConfig()


def log_event(uuid: str, kind: str, **fields) -> None:
    """Append one timestamped event to the call's bot log."""
    line = json.dumps({"wall": time.time(), "kind": kind, **fields})
    with open(os.path.join(CAPTURE_DIR, f"{uuid}-bot.jsonl"), "a") as f:
        f.write(line + "\n")


def tone_frames(seconds: float, rate: int) -> list[OutputAudioRawFrame]:
    """A 440 Hz tone as 20 ms output frames."""
    samples = int(seconds * rate)
    pcm = array.array(
        "h", (int(8000 * math.sin(2 * math.pi * 440 * i / rate)) for i in range(samples))
    )
    step = rate // 50
    return [
        OutputAudioRawFrame(audio=pcm[i : i + step].tobytes(), sample_rate=rate, num_channels=1)
        for i in range(0, samples, step)
    ]


class InputCounter(FrameProcessor):
    """Counts caller audio frames reaching the pipeline."""

    def __init__(self):
        super().__init__()
        self.frames = 0
        self.bytes = 0

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame):
            self.frames += 1
            self.bytes += len(frame.audio)
        await self.push_frame(frame, direction)


@app.post("/config")
async def set_config(new_config: BotConfig) -> BotConfig:
    """Set the scenario for the next calls."""
    global config
    config = new_config
    return config


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    """Run one call: the first message from mod_audio_stream is the channel UUID."""
    await websocket.accept()
    uuid = (await websocket.receive_text()).strip()
    scenario = config
    log_event(uuid, "connected", config=scenario.model_dump())

    serializer = FreeSWITCHFrameSerializer(
        call_uuid=uuid,
        params=FreeSWITCHFrameSerializer.InputParams(
            freeswitch_in_sample_rate=scenario.in_rate,
            freeswitch_out_sample_rate=scenario.out_rate,
        ),
    )
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_out_10ms_chunks=scenario.audio_out_10ms_chunks,
            audio_out_end_silence_secs=0,
            serializer=serializer,
        ),
    )
    counter = InputCounter()
    worker = PipelineWorker(
        Pipeline([transport.input(), counter, transport.output()]),
        params=PipelineParams(
            audio_in_sample_rate=scenario.in_rate, audio_out_sample_rate=scenario.pipeline_out_rate
        ),
    )

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        frames = tone_frames(scenario.tone_seconds, scenario.pipeline_out_rate)
        log_event(uuid, "tone_queued", seconds=scenario.tone_seconds, frames=len(frames))
        if scenario.scenario == "speak_end":
            await worker.queue_frames([*frames, EndFrame()])
            log_event(uuid, "end_queued")
        else:
            await worker.queue_frames(frames)
            await asyncio.sleep(scenario.interrupt_after_s)
            log_event(uuid, "interrupt")
            await worker.queue_frame(InterruptionWorkerFrame())
            await asyncio.sleep(2.0)
            log_event(uuid, "end_queued")
            await worker.queue_frame(EndFrame())

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        log_event(uuid, "client_disconnected")
        await worker.cancel()

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    await runner.run()
    log_event(uuid, "finished", input_frames=counter.frames, input_bytes=counter.bytes)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="info")
