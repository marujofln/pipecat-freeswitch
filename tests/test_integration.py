#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""In-process integration tests: a real Pipecat pipeline behind FastAPIWebsocketTransport.

uvicorn serves the bot on a free port. A WebSocket client plays mod_audio_stream: it sends the
channel UUID and caller audio, and turns every ``streamAudio`` message into a
``mod_audio_stream::play`` event on the fake event socket, which the serializer then plays with
``sendmsg``. No FreeSWITCH is needed, but the transport, the pipeline and the serializer are the
real ones, so a change in how Pipecat drives serializers shows up here.
"""

import asyncio
import base64
import json
from collections.abc import Awaitable, Callable

import pytest
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
from websockets.asyncio.client import connect

from fake_esl import play_path
from pipecat_freeswitch import FreeSWITCHFrameSerializer

pytestmark = pytest.mark.integration

UUID = "11111111-2222-3333-4444-555555555555"
STREAM_RATE = 8000  # uuid_audio_stream ... mono 8k
PIPELINE_IN_RATE = 16000

OnConnected = Callable[[PipelineWorker], Awaitable[None]]


class Recorder(FrameProcessor):
    """Counts the caller audio that reaches the pipeline."""

    def __init__(self):
        super().__init__()
        self.audio_bytes = 0
        self.sample_rates: set[int] = set()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame):
            self.audio_bytes += len(frame.audio)
            self.sample_rates.add(frame.sample_rate)
        await self.push_frame(frame, direction)


def bot_app(esl_port: int, recorder: Recorder, on_connected: OnConnected | None = None) -> FastAPI:
    """A bot built like examples/bot.py, with the AI services left out."""
    app = FastAPI()

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        call_uuid = (await websocket.receive_text()).strip()
        serializer = FreeSWITCHFrameSerializer(
            call_uuid=call_uuid,
            esl_port=esl_port,
            params=FreeSWITCHFrameSerializer.InputParams(hang_up_timeout_secs=5),
        )
        transport = FastAPIWebsocketTransport(
            websocket=websocket,
            params=FastAPIWebsocketParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_out_10ms_chunks=4,
                audio_out_end_silence_secs=0,
                serializer=serializer,
            ),
        )
        worker = PipelineWorker(
            Pipeline([transport.input(), recorder, transport.output()]),
            params=PipelineParams(
                audio_in_sample_rate=PIPELINE_IN_RATE, audio_out_sample_rate=STREAM_RATE
            ),
        )

        @transport.event_handler("on_client_connected")
        async def on_client_connected(transport, client):
            if on_connected:
                await on_connected(worker)

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport, client):
            await worker.cancel()
            await serializer.cleanup()

        runner = WorkerRunner(handle_sigint=False)
        await runner.add_workers(worker)
        await runner.run()

    return app


@pytest.fixture
async def serve():
    """Serve an app with uvicorn on a free port; returns the port."""
    running: list[tuple[uvicorn.Server, asyncio.Task]] = []

    async def _serve(app: FastAPI) -> int:
        server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="off")
        )
        task = asyncio.create_task(server.serve())
        running.append((server, task))
        while not server.started:
            if task.done():
                task.result()
            await asyncio.sleep(0.01)
        return server.servers[0].sockets[0].getsockname()[1]

    yield _serve
    for server, task in running:
        server.should_exit = True
        await asyncio.wait_for(task, 5)


async def mod_audio_stream(
    port: int, esl_server, caller_audio: list[bytes] = (), timeout: float = 10
) -> list[dict]:
    """Act as mod_audio_stream for one call until the bot closes the WebSocket.

    Returns:
        The ``streamAudio`` messages received, in order.
    """
    messages: list[dict] = []
    async with connect(f"ws://127.0.0.1:{port}/ws") as ws:
        await ws.send(UUID)
        for chunk in caller_audio:
            await ws.send(chunk)
        async with asyncio.timeout(timeout):
            async for raw in ws:
                message = json.loads(raw)
                if message.get("type") == "streamAudio":
                    # mod_audio_stream writes <uuid>_<n>.tmp.r8 and announces it.
                    await esl_server.emit_play(UUID, len(messages))
                    messages.append(message)
    return messages


def tone(seconds: float, rate: int = STREAM_RATE) -> list[OutputAudioRawFrame]:
    """Constant non-silent audio as 20 ms frames."""
    frame = b"\x10\x00" * (rate // 50)
    return [
        OutputAudioRawFrame(audio=frame, sample_rate=rate, num_channels=1)
        for _ in range(int(seconds * 50))
    ]


def audio_bytes(messages: list[dict]) -> int:
    return sum(len(base64.b64decode(m["data"]["audioData"])) for m in messages)


def sendmsgs(esl_server) -> list[str]:
    return [c for c in esl_server.commands if c.startswith("sendmsg")]


async def test_bot_speech_is_played_then_the_call_hangs_up(esl_server, serve):
    esl_server.auto_complete_playback = True

    async def speak(worker: PipelineWorker):
        await worker.queue_frames([*tone(1.0), EndFrame()])

    port = await serve(bot_app(esl_server.port, Recorder(), speak))
    messages = await mod_audio_stream(port, esl_server)

    # 1 s at 8 kHz, in 40 ms blocks (audio_out_10ms_chunks=4).
    assert audio_bytes(messages) == 16000
    assert {m["data"]["sampleRate"] for m in messages} == {8000}
    assert {len(base64.b64decode(m["data"]["audioData"])) for m in messages} == {640}
    # Every block is queued for playback, in order, and the hang-up comes after the last one.
    await esl_server.wait_for_command("api uuid_kill")
    queued = sendmsgs(esl_server)
    assert [play_path(UUID, i) in c for i, c in enumerate(queued)] == [True] * len(messages)
    last_sendmsg = esl_server.commands.index(queued[-1])
    assert esl_server.commands.index(f"api uuid_kill {UUID}") > last_sendmsg
    await esl_server.wait_until_all_closed()


async def test_caller_audio_reaches_the_pipeline(esl_server, serve):
    recorder = Recorder()
    port = await serve(bot_app(esl_server.port, recorder))
    # 1 s of caller audio at the stream rate, as mod_audio_stream sends it (20 ms frames).
    caller_audio = [b"\x10\x00" * (STREAM_RATE // 50)] * 50

    async def call():
        async with connect(f"ws://127.0.0.1:{port}/ws") as ws:
            await ws.send(UUID)
            for chunk in caller_audio:
                await ws.send(chunk)
            await asyncio.sleep(0.5)  # let the pipeline drain before the caller hangs up

    await call()
    assert recorder.sample_rates == {PIPELINE_IN_RATE}
    # About 1 s at 16 kHz; the stream resampler holds back up to ~150 ms.
    assert 32000 - 2 * 16 * 150 <= recorder.audio_bytes <= 32000
    # The WebSocket closed first, so the transport never passes CancelFrame to the serializer:
    # serializer.cleanup() in on_client_disconnected hangs up and closes the event socket.
    await esl_server.wait_for_command(f"api uuid_kill {UUID}")
    await esl_server.wait_until_all_closed()


async def test_interruption_stops_playback_and_the_call_still_ends(esl_server, serve):
    async def speak_then_interrupt(worker: PipelineWorker):
        await worker.queue_frames(tone(3.0))
        await asyncio.sleep(0.5)
        await worker.queue_frame(InterruptionWorkerFrame())
        await asyncio.sleep(0.3)
        await worker.queue_frame(EndFrame())

    port = await serve(bot_app(esl_server.port, Recorder(), speak_then_interrupt))
    messages = await mod_audio_stream(port, esl_server)

    assert f"api uuid_break {UUID} all" in esl_server.commands
    # The rest of the 3 s tone was dropped from the pipeline instead of being sent.
    assert 0 < audio_bytes(messages) < 3 * 16000
    # Playback that never completes was flushed by uuid_break, so the hang-up does not wait for it.
    await esl_server.wait_for_command(f"api uuid_kill {UUID}")
    await esl_server.wait_until_all_closed()
