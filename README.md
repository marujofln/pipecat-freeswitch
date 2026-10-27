# pipecat-freeswitch

Community integration that connects [Pipecat](https://github.com/pipecat-ai/pipecat) voice-agent pipelines to
[FreeSWITCH](https://signalwire.com/freeswitch) through the open source
[`mod_audio_stream`](https://github.com/amigniter/mod_audio_stream) WebSocket module.

`FreeSWITCHFrameSerializer` plugs into Pipecat's `FastAPIWebsocketTransport`: caller audio flows into the pipeline,
bot audio is played on the call, and barge-in and hang-up work.

> **Status: v0.1.0, alpha.** Tested end to end against FreeSWITCH 1.11.3 with a Pipecat pipeline (tone playback,
> hang-up after speech, interruption) and with unit tests. The full STT → LLM → TTS example has been built but not
> run against live AI services. Not yet published on PyPI.

## How it works

```
Caller ──SIP/RTP──> FreeSWITCH ──(mod_audio_stream, WebSocket)──> FastAPIWebsocketTransport
                       ▲  │                                           │ FreeSWITCHFrameSerializer
                       │  └─ event mod_audio_stream::play ─┐          ▼
                       │                                   ▼
                       └── ESL: sendmsg execute playback <── FreeSWITCHControl (inbound ESL)
                           uuid_break all, uuid_kill
                                                                    pipeline: VAD → STT → LLM → TTS
```

- **Caller audio**: mod_audio_stream sends binary L16 frames; the serializer turns them into `InputAudioRawFrame`.
- **Bot audio**: each output frame becomes a `streamAudio` JSON message. The open source mod_audio_stream only writes
  it to a file and raises `mod_audio_stream::play`; **it does not play it**. The serializer therefore keeps an event
  socket (ESL) connection per call and queues `playback` of each file with `sendmsg`. (`uuid_broadcast` would add
  100 ms of silence before every file.)
- **Barge-in**: on `InterruptionFrame`, `uuid_break <uuid> all` stops playback and clears the queue; audio still in
  flight from before the interruption is dropped.
- **Hang-up**: on `EndFrame`, the serializer waits until the queued audio has finished playing, then runs
  `uuid_kill`; on `CancelFrame` it hangs up at once.
- **Channel UUID**: the last argument of `uuid_audio_stream ... start`, delivered as the first WebSocket message.

## Installation

```bash
pip install "pipecat-freeswitch @ git+https://github.com/marujofln/pipecat-freeswitch"
# or, with uv
uv add "pipecat-freeswitch @ git+https://github.com/marujofln/pipecat-freeswitch"
```

The ESL client is built on asyncio streams; the only dependency is `pipecat-ai`. Install the Pipecat extras your
pipeline needs (`websocket` for the transport, plus your STT/LLM/TTS services).

## FreeSWITCH setup

1. **mod_audio_stream** (community edition), loaded in `modules.conf.xml`. The bench builds it from source:
   see [`examples/freeswitch/Dockerfile`](examples/freeswitch/Dockerfile).
2. **mod_event_socket**, reachable from the bot. The event socket grants full control of FreeSWITCH: keep it on a
   private network, restrict it with an ACL (`apply-inbound-acl`) and set a strong password.
3. **SIP profiles with an RTP timer** (`<param name="rtp-timer-name" value="soft"/>`, as in the vanilla config).
4. **Dialplan**: answer, start the stream with the channel UUID as metadata, and park:

```xml
<extension name="pipecat">
  <condition field="destination_number" expression="^9999$">
    <action application="answer"/>
    <action application="set"
      data="stream_result=${uuid_audio_stream(${uuid} start ws://BOT_HOST:8765/ws mono 8k ${uuid})}"/>
    <action application="park"/>
  </condition>
</extension>
```

## Usage

```python
from fastapi import WebSocket
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport

from pipecat_freeswitch import FreeSWITCHFrameSerializer


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    call_uuid = (await websocket.receive_text()).strip()  # first message: the channel UUID

    serializer = FreeSWITCHFrameSerializer(
        call_uuid=call_uuid,
        esl_host="10.0.0.5",
        esl_port=8021,
        esl_password="ClueCon",
        params=FreeSWITCHFrameSerializer.InputParams(freeswitch_sample_rate=8000),
    )
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_out_10ms_chunks=4,       # 40 ms per streamAudio block
            audio_out_end_silence_secs=0,  # the serializer waits for playback before hanging up
            serializer=serializer,
        ),
    )
    # Build the pipeline with transport.input() ... transport.output() and run it, with
    # PipelineParams(audio_in_sample_rate=8000, audio_out_sample_rate=8000).
```

`call_uuid` is optional: without it the serializer takes the UUID from the first message and holds bot audio until
the event socket is ready. Reading it yourself, as above, lets the serializer connect during pipeline setup.

### Parameters

| `InputParams` field | Default | Meaning |
|---|---|---|
| `freeswitch_sample_rate` | `8000` | Default rate for both directions |
| `freeswitch_in_sample_rate` | `freeswitch_sample_rate` | Caller audio rate, as given to `uuid_audio_stream` (`8k`, `16k`) |
| `freeswitch_out_sample_rate` | pipeline output rate | Bot audio rate: 8000, 16000, 24000, 32000 (raw) or 48000 (sent as WAV) |
| `output_resampler_quality` | `"QQ"` | SOXR preset used only if bot audio must be resampled |
| `sample_rate` | `None` | Override for the pipeline input rate |
| `auto_hang_up` | `True` | Hang up on `EndFrame` (after playback) and `CancelFrame` (at once) |
| `hang_up_timeout_secs` | `30.0` | Maximum wait for queued audio on `EndFrame` |

Constructor: `call_uuid`, `esl_host` (`127.0.0.1`), `esl_port` (`8021`), `esl_password` (`ClueCon`), `params`, and
optionally `control` (a `FreeSWITCHControl` to use instead of one built from the `esl_*` arguments).

### Sample rates and codecs

Send bot audio **at the channel's rate**: FreeSWITCH resamples each played file on its own, which leaves an artefact
at every block seam (on the bench, 71 glitchy 10 ms windows out of 286 with 40 ms blocks, against 0 at the channel
rate). The simplest setup runs the pipeline output at that rate, so nothing is resampled anywhere:

| Channel codec | `uuid_audio_stream` | `PipelineParams` | Serializer |
|---|---|---|---|
| PCMU / PCMA | `mono 8k` | `audio_in_sample_rate=8000, audio_out_sample_rate=8000` | `freeswitch_in_sample_rate=8000` |
| Opus (48 kHz inside FreeSWITCH) | `mono 16k` | `audio_in_sample_rate=16000, audio_out_sample_rate=48000` | `freeswitch_in_sample_rate=16000` |
| G.722 | `mono 16k` | `audio_in_sample_rate=16000, audio_out_sample_rate=16000` | `freeswitch_in_sample_rate=16000` |

- **Caller audio** is resampled by mod_audio_stream from the channel rate to the stream rate. With Opus, `16k` keeps
  wideband speech for STT and VAD (Silero accepts 8 or 16 kHz); `8k` throws Opus's extra bandwidth away.
- **Bot audio** at 48 kHz is sent as WAV, because FreeSWITCH plays raw audio only at 8, 16, 24 and 32 kHz.
- If the pipeline output rate differs from `freeswitch_out_sample_rate`, the serializer resamples with SOXR "QQ":
  higher presets hold back up to ~110 ms, and since nothing flushes the resampler after an utterance, its end would
  be lost. "QQ" has the weakest anti-aliasing; prefer letting the TTS produce the channel rate.

### Choosing the block size

Each output chunk of the transport (`audio_out_10ms_chunks` × 10 ms) becomes one `streamAudio` message, one temporary
file and one `playback` in FreeSWITCH. Smaller blocks cut latency; larger blocks cut per-file overhead. On the bench,
40 ms and 200 ms blocks both played gaplessly at the channel rate, with first audio 50–150 ms after the bot queued it;
FreeSWITCH used about 1.0–1.3 s of CPU for a 3 s call with 40 ms blocks against 0.55–0.85 s with 200 ms blocks.

### Known limitations

- No DTMF yet (planned for v0.2).
- Pipecat's WebSocket transport sends audio about twice as fast as real time, so FreeSWITCH queues ahead of playback
  and `BotStoppedSpeakingFrame` can arrive before the caller has heard the end of the speech.
- The serializer relies on mod_audio_stream naming files `<uuid>_<n>.tmp.<ext>` in send order to drop stale audio
  after an interruption; this was verified on commit `da8b068`.

## Example

[`examples/bot.py`](examples/bot.py) is a complete voice bot (Deepgram STT, OpenAI LLM, Cartesia TTS, Silero VAD):

```bash
export DEEPGRAM_API_KEY=... OPENAI_API_KEY=... CARTESIA_API_KEY=...
export FREESWITCH_ESL_HOST=10.0.0.5 FREESWITCH_ESL_PASSWORD=ClueCon
uv run --with "pipecat-ai[websocket,deepgram,openai,cartesia,silero]" --with python-dotenv --with uvicorn \
  examples/bot.py
```

Point the dialplan's `uuid_audio_stream` at `ws://BOT_HOST:8765/ws mono 8k ${uuid}`.

## Development

```bash
uv venv && uv pip install -e . pytest pytest-asyncio ruff
.venv/bin/python -m pytest          # unit tests (fake event socket, no FreeSWITCH needed)
.venv/bin/ruff check src tests && .venv/bin/ruff format --check src tests
```

[`examples/freeswitch/`](examples/freeswitch/) is a test bench: FreeSWITCH 1.11.3 built from source with
mod_audio_stream, a bench-only SIP profile so FreeSWITCH can call itself, and experiment scripts. End-to-end test of
the serializer with a real Pipecat pipeline:

```bash
cd examples/freeswitch && docker compose up -d && cd ../..
uv pip install "pipecat-ai[websocket]" fastapi "uvicorn[standard]"
.venv/bin/python bench/tone_bot.py &   # Pipecat bot on extension 9998
python3 bench/run_e2e.py               # tone + hang-up after speech, and interruption
```

The bench README documents the phase 0 measurements that shaped the design: why playback uses `sendmsg`, why
`uuid_break` needs `all`, and why hang-up waits for playback.

## Tested versions

Pipecat 1.14.0, FreeSWITCH 1.11.3 (phase 0 results also reproduced on 1.10.12), mod_audio_stream `da8b068`
(community edition), Python 3.12. Codecs tested end to end: PCMU and Opus.

## License

BSD-2-Clause, same as Pipecat. See [LICENSE](LICENSE).
