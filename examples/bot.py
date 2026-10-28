#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Voice bot answering FreeSWITCH calls through mod_audio_stream.

FreeSWITCH dialplan (the last argument is the channel UUID, sent as the first message)::

    <action application="answer"/>
    <action application="set" data='STREAM_EXTRA_HEADERS={"Authorization":"Bearer $${bot_token}"}'/>
    <action application="set"
      data="r=${uuid_audio_stream(${uuid} start ws://BOT_HOST:8765/ws mono 8k ${uuid})}"/>
    <action application="park"/>

The WebSocket only accepts connections that carry BOT_TOKEN as a bearer token: otherwise anyone who
can reach it could send another call's UUID and interrupt or hang up that call. ``bot_token`` is
set in vars.xml (``<X-PRE-PROCESS cmd="set" data="bot_token=..."/>``). Use ``wss://`` or a private
network, since over plain ``ws://`` the token travels in clear text.

The bot also needs the FreeSWITCH event socket (mod_event_socket) to play audio, barge in and
hang up. Environment: BOT_TOKEN, DEEPGRAM_API_KEY, OPENAI_API_KEY, and optionally
FREESWITCH_ESL_HOST, FREESWITCH_ESL_PORT, FREESWITCH_ESL_PASSWORD, HOST (0.0.0.0) and PORT (8765).

The text-to-speech service is chosen with TTS_PROVIDER:

- ``cartesia`` (default): CARTESIA_API_KEY, optionally CARTESIA_VOICE_ID.
- ``elevenlabs``: ELEVENLABS_API_KEY and ELEVENLABS_VOICE_ID (required: the service has no
  default voice).

Run with the extra of the provider you use::

    uv run --with "pipecat-ai[websocket,deepgram,openai,cartesia,silero]" examples/bot.py
    TTS_PROVIDER=elevenlabs \
        uv run --with "pipecat-ai[websocket,deepgram,openai,elevenlabs,silero]" examples/bot.py

Both services synthesize at the pipeline output rate (OUTPUT_RATE below), so audio reaches
FreeSWITCH without resampling.
"""

import os
import secrets

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)
from pipecat.workers.runner import WorkerRunner

from pipecat_freeswitch import FreeSWITCHFrameSerializer

load_dotenv(override=True)

# Caller audio: the rate given to uuid_audio_stream ("8k" -> 8000, "16k" -> 16000).
STREAM_RATE = 8000
# Bot audio: the channel's codec rate, so neither the serializer nor FreeSWITCH resamples it.
# 8000 for PCMU/PCMA. For Opus use STREAM_RATE = 16000 ("16k") and OUTPUT_RATE = 48000.
OUTPUT_RATE = 8000

# Shared secret that FreeSWITCH sends in the Authorization header (STREAM_EXTRA_HEADERS).
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN to the token FreeSWITCH sends (bot_token in vars.xml)")

# Fail at startup, not in the middle of the first call, when a service key is missing.
_TTS_KEYS = {
    "cartesia": ["CARTESIA_API_KEY"],
    "elevenlabs": ["ELEVENLABS_API_KEY", "ELEVENLABS_VOICE_ID"],
}
_missing = [
    name
    for name in ["DEEPGRAM_API_KEY", "OPENAI_API_KEY"]
    + _TTS_KEYS.get(os.getenv("TTS_PROVIDER", "cartesia").lower(), [])
    if not os.getenv(name)
]
if _missing:
    raise RuntimeError(f"Set {', '.join(_missing)} (see .env.example)")

app = FastAPI()


def is_authorized(websocket: WebSocket) -> bool:
    """Whether the handshake carries the bot token, compared in constant time."""
    auth = websocket.headers.get("authorization", "")
    return secrets.compare_digest(auth.encode(), f"Bearer {BOT_TOKEN}".encode())


def build_tts():
    """Create the text-to-speech service selected by TTS_PROVIDER.

    The provider modules are imported here so only the extra you use needs to be installed.
    """
    provider = os.getenv("TTS_PROVIDER", "cartesia").lower()
    if provider == "cartesia":
        from pipecat.services.cartesia.tts import CartesiaTTSService

        return CartesiaTTSService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            settings=CartesiaTTSService.Settings(
                voice=os.getenv("CARTESIA_VOICE_ID", "71a7ad14-091c-4e8e-a314-022ece01c121")
            ),
        )
    if provider == "elevenlabs":
        from pipecat.services.elevenlabs.tts import ElevenLabsTTSService

        voice_id = os.getenv("ELEVENLABS_VOICE_ID")
        if not voice_id:
            raise RuntimeError("TTS_PROVIDER=elevenlabs requires ELEVENLABS_VOICE_ID")
        return ElevenLabsTTSService(
            api_key=os.getenv("ELEVENLABS_API_KEY"),
            settings=ElevenLabsTTSService.Settings(voice=voice_id),
        )
    raise RuntimeError(f"Unknown TTS_PROVIDER {provider!r}; use 'cartesia' or 'elevenlabs'")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handle one call."""
    if not is_authorized(websocket):
        logger.warning(f"Rejected WebSocket from {websocket.client}: missing or wrong token")
        await websocket.close(code=1008)  # before accept(): the handshake fails with HTTP 403
        return
    await websocket.accept()
    # mod_audio_stream sends the metadata (here, the channel UUID) before any audio.
    call_uuid = (await websocket.receive_text()).strip()
    logger.info(f"Call {call_uuid} connected")
    await run_bot(websocket, call_uuid)


async def run_bot(websocket: WebSocket, call_uuid: str):
    """Run the voice pipeline for one call."""
    serializer = FreeSWITCHFrameSerializer(
        call_uuid=call_uuid,
        esl_host=os.getenv("FREESWITCH_ESL_HOST", "127.0.0.1"),
        esl_port=int(os.getenv("FREESWITCH_ESL_PORT", "8021")),
        esl_password=os.getenv("FREESWITCH_ESL_PASSWORD", "ClueCon"),
        params=FreeSWITCHFrameSerializer.InputParams(
            freeswitch_in_sample_rate=STREAM_RATE, freeswitch_out_sample_rate=OUTPUT_RATE
        ),
    )

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            # Each output chunk becomes one file played by FreeSWITCH: 4 x 10 ms = 40 ms blocks.
            audio_out_10ms_chunks=4,
            # No trailing silence: the serializer already waits for playback before hanging up.
            audio_out_end_silence_secs=0,
            serializer=serializer,
        ),
    )

    stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))
    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        settings=OpenAILLMService.Settings(model="gpt-4.1-mini"),
    )
    tts = build_tts()

    context = LLMContext(
        [
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant on a phone call. Keep answers short and "
                    "conversational; your output is spoken, so avoid lists and formatting."
                ),
            }
        ]
    )
    aggregators = LLMContextAggregatorPair(
        context, user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer())
    )

    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            aggregators.user(),
            llm,
            tts,
            transport.output(),
            aggregators.assistant(),
        ]
    )

    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(audio_in_sample_rate=STREAM_RATE, audio_out_sample_rate=OUTPUT_RATE),
    )

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        context.add_message({"role": "user", "content": "Greet the caller briefly."})
        await worker.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info(f"Call {call_uuid} disconnected")
        await worker.cancel()
        # The closed WebSocket keeps CancelFrame from reaching the serializer: end the call here.
        await serializer.cleanup()

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    await runner.run()


if __name__ == "__main__":
    uvicorn.run(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8765")))
