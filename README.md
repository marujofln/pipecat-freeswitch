# pipecat-freeswitch

Community integration that connects [Pipecat](https://github.com/pipecat-ai/pipecat) voice-agent pipelines to
[FreeSWITCH](https://signalwire.com/freeswitch) through the open source
[`mod_audio_stream`](https://github.com/amigniter/mod_audio_stream) WebSocket module.

`FreeSWITCHFrameSerializer` plugs into Pipecat's `FastAPIWebsocketTransport`: caller audio flows into the pipeline,
bot audio is played on the call, and barge-in and hang-up work.

This is an independent community project, not affiliated with SignalWire (FreeSWITCH), Daily (Pipecat) or the
mod_audio_stream authors.

> **Status: v0.1.0, alpha.** Tested end to end against FreeSWITCH 1.11.3 with a Pipecat pipeline (tone playback,
> hang-up after speech, interruption), with unit and integration tests, and with calls from a softphone (Linphone)
> and a desk phone (Grandstream) to the full STT → LLM → TTS example (all three on OpenRouter), barge-in included.
> Deepgram, OpenAI, Cartesia and ElevenLabs direct have not been run against their live APIs yet.

## Demo

A phone call to the [example bot](#example) on the test bench (extension 9996; STT, LLM and TTS through
OpenRouter). The caller interrupts the bot twice, and each time the bot goes silent about 0.4 s after the caller
starts talking. The video is FreeSWITCH's stereo recording of the call (caller on top, bot below), with captions
taken from the bot's log.

<!-- Drag demo.mp4 here in the GitHub editor: it becomes a https://github.com/user-attachments/assets/... link
     that plays inline. -->

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
uv add pipecat-freeswitch
# or
pip install pipecat-freeswitch
```

The ESL client is built on asyncio streams; the only dependency is `pipecat-ai`. Install the Pipecat extras your
pipeline needs (`websocket` for the transport, plus your STT/LLM/TTS services).

## FreeSWITCH setup

1. **mod_audio_stream** (community edition), loaded in `modules.conf.xml`. The bench builds it from source:
   see [`examples/freeswitch/Dockerfile`](examples/freeswitch/Dockerfile).
2. **mod_event_socket**, reachable from the bot. The event socket grants full control of FreeSWITCH: keep it on a
   private network, restrict it with an ACL (`apply-inbound-acl`) and set a strong password.
3. **SIP profiles with an RTP timer** (`<param name="rtp-timer-name" value="soft"/>`, as in the vanilla config).
4. **Dialplan**: answer, set the bot's token, start the stream with the channel UUID as metadata, and park:

```xml
<extension name="pipecat">
  <condition field="destination_number" expression="^9999$">
    <action application="answer"/>
    <action application="set" data='STREAM_EXTRA_HEADERS={"Authorization":"Bearer $${bot_token}"}'/>
    <action application="set"
      data="stream_result=${uuid_audio_stream(${uuid} start ws://BOT_HOST:8765/ws mono 8k ${uuid})}"/>
    <action application="park"/>
  </condition>
</extension>
```

with the token in `vars.xml` (a long random value, e.g. from `openssl rand -hex 32`):

```xml
<X-PRE-PROCESS cmd="set" data="bot_token=..."/>
```

### Securing the WebSocket

The serializer controls the call named by the first WebSocket message. Without authentication, anyone who can reach
the bot's WebSocket could send another call's UUID and interrupt or hang up that call. (The serializer accepts only
canonical UUIDs, so the message cannot inject event socket commands.)

- **Require a token.** mod_audio_stream sends the headers in the `STREAM_EXTRA_HEADERS` channel variable (a JSON
  object) with the handshake. Check the `Authorization` header **before** `websocket.accept()`, with
  `secrets.compare_digest`, and close with code 1008 otherwise; the handshake then fails with HTTP 403. A header
  is preferable to a `?token=` query string, which ends up in FreeSWITCH and proxy logs.
- **Encrypt the link.** Over plain `ws://` the token travels in clear text: use `wss://` (mod_audio_stream's
  `STREAM_TLS_*` variables) or keep FreeSWITCH and the bot on a private network.
- A single shared token lets whoever holds it name any call. A per-call token (an HMAC of the channel UUID, checked
  against the first message) would limit a leak to one call; it is not implemented yet.

#### Managing the token

The same value goes in two places: `bot_token` in FreeSWITCH's `vars.xml`, which sends it, and `BOT_TOKEN` in the
bot's environment, which checks it.

- **Generate** a long random hex value: `openssl rand -hex 32` or
  `python3 -c "import secrets; print(secrets.token_hex(32))"`. Stick to letters and digits: the value goes through
  an XML attribute, a JSON string in `STREAM_EXTRA_HEADERS` and an HTTP header, where `"`, `&`, `<`, `\`, `$` or
  `{}` would break one of them.
- **FreeSWITCH**: `$${bot_token}` is expanded when the XML is loaded, so run `fs_cli -x reloadxml` (or restart)
  after changing it. Make `vars.xml` readable only by the FreeSWITCH user (e.g. `chmod 640`).
- **Bot**: `examples/bot.py` reads `BOT_TOKEN` from the environment or from a `.env` file (`python-dotenv`), which
  is git-ignored. Restart the bot after changing it.
- **Never commit the real token.** `bench-token` in the bench's `vars.xml` is for the test bench only, and is what
  `bench/tone_bot.py` uses when `BOT_TOKEN` is unset.
- **Visibility**: the token is a channel variable, so it shows up in `uuid_dump` and possibly in CDRs. Only those
  with fs_cli or event socket access can see it, and they already have full control of FreeSWITCH.
- **Rotation**: change both sides and restart or reload them. While the two values differ, new calls are refused
  (HTTP 403); calls in progress are not affected, since the token is only checked on connection. To rotate with no
  refused calls, the bot would have to accept the old and the new token for a while; this is not implemented.

## Usage

```python
import secrets

from fastapi import WebSocket
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport

from pipecat_freeswitch import FreeSWITCHFrameSerializer

BOT_TOKEN = "..."  # the bot_token of vars.xml, e.g. from an environment variable


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    auth = websocket.headers.get("authorization", "")
    if not secrets.compare_digest(auth.encode(), f"Bearer {BOT_TOKEN}".encode()):
        await websocket.close(code=1008)  # refuse the handshake (HTTP 403)
        return
    await websocket.accept()
    call_uuid = (await websocket.receive_text()).strip()  # first message: the channel UUID

    serializer = FreeSWITCHFrameSerializer(
        call_uuid=call_uuid,
        esl_host="10.0.0.5",
        esl_port=8021,
        esl_password="ClueCon",
        params=FreeSWITCHFrameSerializer.InputParams(
            freeswitch_sample_rate=8000, output_gain_db=-12  # TTS is far louder than phone level
        ),
    )
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_out_10ms_chunks=20,      # 200 ms blocks: shorter ones deafen barge-in
            audio_out_end_silence_secs=0,  # the serializer waits for playback before hanging up
            serializer=serializer,
        ),
    )
    # Build the pipeline with transport.input() ... transport.output() and run it, with
    # PipelineParams(audio_in_sample_rate=8000, audio_out_sample_rate=8000).

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        await worker.cancel()
        await serializer.cleanup()  # hang up and close the event socket
```

Call `serializer.cleanup()` when the WebSocket closes. Once it is closed, the transport stops passing frames to the
serializer, so the `CancelFrame` that ends the pipeline never reaches it; without `cleanup()`, a WebSocket that drops
while the call is up leaves the call parked and the event socket connection open until the call ends. This is a gap
in Pipecat's transport, which affects its own telephony serializers too, reported in
[pipecat-ai/pipecat#6440](https://github.com/pipecat-ai/pipecat/issues/6440).

`call_uuid` is optional: without it the serializer takes the UUID from the first message and holds bot audio until
the event socket is ready. Reading it yourself, as above, lets the serializer connect during pipeline setup.

### Parameters

| `InputParams` field | Default | Meaning |
|---|---|---|
| `freeswitch_sample_rate` | `8000` | Default rate for both directions |
| `freeswitch_in_sample_rate` | `freeswitch_sample_rate` | Caller audio rate, as given to `uuid_audio_stream` (`8k`, `16k`) |
| `freeswitch_out_sample_rate` | `freeswitch_sample_rate` | Bot audio rate, the channel's: 8000, 16000, 24000, 32000 (raw) or 48000 (sent as WAV) |
| `output_resampler_quality` | `"QQ"` | SOXR preset used only if bot audio must be resampled |
| `output_gain_db` | `0.0` | Gain applied to bot audio, in dB (negative attenuates); see [Bot audio level](#bot-audio-level) |
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
| PCMU / PCMA | `mono 8k` | `audio_in_sample_rate=8000, audio_out_sample_rate=8000` | defaults (`freeswitch_sample_rate=8000`) |
| Opus (48 kHz inside FreeSWITCH) | `mono 16k` | `audio_in_sample_rate=16000, audio_out_sample_rate=48000` | `freeswitch_in_sample_rate=16000, freeswitch_out_sample_rate=48000` |
| G.722 | `mono 16k` | `audio_in_sample_rate=16000, audio_out_sample_rate=16000` | `freeswitch_sample_rate=16000` |

- **Caller audio** is resampled by mod_audio_stream from the channel rate to the stream rate. With Opus, `16k` keeps
  wideband speech for STT and VAD (Silero accepts 8 or 16 kHz); `8k` throws Opus's extra bandwidth away.
- **Bot audio** at 48 kHz is sent as WAV, because FreeSWITCH plays raw audio only at 8, 16, 24 and 32 kHz.
- Bot audio is always sent at `freeswitch_out_sample_rate` (8000 unless set), never at the pipeline rate as such:
  a 24 kHz TTS on a G.711 channel is resampled to 8 kHz by the serializer, which on the bench gave no glitches,
  instead of being resampled block by block by FreeSWITCH.
- If the pipeline output rate differs from `freeswitch_out_sample_rate`, the serializer resamples with SOXR "QQ":
  higher presets hold back up to ~110 ms, and since nothing flushes the resampler after an utterance, its end would
  be lost. "QQ" has the weakest anti-aliasing; prefer letting the TTS produce the channel rate.

### Choosing the block size

Each output chunk of the transport (`audio_out_10ms_chunks` × 10 ms) becomes one `streamAudio` message, one temporary
file and one `playback` in FreeSWITCH. **Use 200 ms or more (`audio_out_10ms_chunks=20`), not 40 ms**, or the bot
cannot hear the caller while it speaks:

- **Every `playback` that starts deafens the channel for 30–40 ms.** FreeSWITCH's `playback` sends an audio sync when
  it starts (`switch_ivr_play_file`), and on a SIP channel that resets the jitter buffer and flushes the caller's
  incoming audio (`SWITCH_MESSAGE_INDICATE_AUDIO_SYNC` in `switch_core_media.c`). No setting turns it off. Measured on
  the bench, with a caller sending a continuous tone while blocks play: **40 ms blocks lose ~100% of the caller's
  audio** (the read side is flushed 25 times a second and never refills), 200 ms blocks 14–16%, 1 s blocks 4%; a
  single long file loses nothing. With 40 ms blocks, barge-in by voice never worked, whatever the phone; with 200 ms
  blocks it stopped the bot 5–6 ms after the caller spoke, on a desk phone and a softphone.
- **Larger blocks cost first-audio latency**, since a block plays only once it is complete (about 160 ms more at
  200 ms than at 40 ms), and save CPU: FreeSWITCH used about 1.0–1.3 s of CPU for a 3 s call with 40 ms blocks against
  0.55–0.85 s with 200 ms blocks. Both played gaplessly at the channel rate.
- **Send the channel's real rate.** Opus may run at 48 or 16 kHz depending on the phone; if `freeswitch_out_sample_rate`
  differs, FreeSWITCH resamples every block on its own and clicks at the seams. The bench's dialplan passes
  `out_rate=${write_rate}` to the bot, which is the rate the channel actually negotiated.

### Bot audio level

TTS services deliver speech much louder than telephone level: on the bench, around −13 dBFS RMS with peaks at full
scale, against about −26 dBFS for active speech on a phone line (ITU-T P.56). That loud, it distorted on a desk phone
and a softphone. `output_gain_db` scales bot audio before it is sent (samples are clipped to 16 bits);
`examples/bot.py` uses −12 dB, which measured −24.7 dBFS RMS with peaks at −12.8 dBFS.

### Known limitations

- No DTMF yet (planned for v0.2).
- Pipecat's WebSocket transport sends audio about twice as fast as real time, so FreeSWITCH queues ahead of playback
  and `BotStoppedSpeakingFrame` can arrive before the caller has heard the end of the speech.
- The serializer relies on mod_audio_stream naming files `<uuid>_<n>.tmp.<ext>` in send order to drop stale audio
  after an interruption; this was verified on commit `da8b068`. If a file name does not follow that scheme, the
  serializer logs a warning and numbers files in the order their `play` events arrive, which is the send order.
- **Temp files stay on disk until the call ends.** mod_audio_stream writes every block of bot speech to
  `<temp_dir>/<uuid>_<n>.tmp.<ext>` (FreeSWITCH's `$${temp_dir}`, usually `/tmp`) and deletes a call's files only when
  its stream stops or the channel closes, not after each one plays. Per minute of bot speech that is 1,500 files with
  40 ms blocks (300 with 200 ms blocks) and about 1 MB at 8 kHz, 2 MB at 16 kHz or 6 MB at 48 kHz, whatever the block
  size. Size the temp directory (space and inodes) for long calls and concurrent calls. If FreeSWITCH crashes, the files
  of the calls in progress stay behind. A periodic cleanup handles them, with an age above your longest call so it
  never touches a live one, e.g. `find /tmp -maxdepth 1 -name '*-*-*-*-*_*.tmp.*' -mmin +240 -delete`. The package
  cannot delete them itself: the files belong to FreeSWITCH, on its host.
- **Barge-in needs blocks of 200 ms or more.** Each block's `playback` flushes 30–40 ms of the caller's incoming audio
  (see [Choosing the block size](#choosing-the-block-size)), so while the bot speaks the caller is partly lost: 14–16%
  with 200 ms blocks, nearly all of it with 40 ms. Once Pipecat detects the caller, the bot's audio stops within a few
  milliseconds (3–11 ms in FreeSWITCH, with the audio still in flight dropped). A fix that keeps the first block
  small and grows the following ones is planned.
- **Phone audio is quiet for Pipecat's default VAD.** A caller speaking normally measured 0.66–0.70 on Pipecat's
  volume scale, so the default `min_volume` of 0.6 misses speech that the flush above has already thinned out;
  `examples/bot.py` uses 0.4. With a loudspeaker and no echo cancellation, raise it again, or the bot may interrupt
  itself on its own echo.

## Example

[`examples/bot.py`](examples/bot.py) is a complete voice bot: speech-to-text, an LLM, Silero VAD and text-to-speech,
each chosen with an environment variable:

| Variable | Options (keys) | Pipecat extra |
|---|---|---|
| `STT_PROVIDER` | `deepgram` (default; `DEEPGRAM_API_KEY`), `openrouter`* (`OPENROUTER_API_KEY`; `STT_MODEL`, default `openai/whisper-1`) | `deepgram`, `openrouter` |
| `LLM_PROVIDER` | `openai` (default; `OPENAI_API_KEY`), `openrouter` (`OPENROUTER_API_KEY`); `LLM_MODEL`, default `gpt-4.1-mini` / `openai/gpt-4.1-mini`; `LLM_MAX_TOKENS`, default 300 | `openai`, `openrouter` |
| `TTS_PROVIDER` | `cartesia` (default; `CARTESIA_API_KEY`, optional `CARTESIA_VOICE_ID`), `elevenlabs` (`ELEVENLABS_API_KEY`, `ELEVENLABS_VOICE_ID`, required: no default voice), `openrouter`* (`OPENROUTER_API_KEY`; `TTS_MODEL`, default `elevenlabs/eleven-flash-v2.5`, `TTS_VOICE`, default `sarah`) | `cartesia`, `elevenlabs`, `openrouter` |

With OpenRouter, one key covers all three, and the LLM can be any model OpenRouter routes to (e.g.
`LLM_MODEL=anthropic/claude-...`). `LLM_MAX_TOKENS` caps each answer: without a cap, OpenRouter reserves credit for
the model's whole output limit (65,536 tokens for `openai/gpt-4.1-mini`) on every request and refuses it with HTTP 402
when the balance is low.

\* **OpenRouter's speech services work request by request**, and Pipecat has no OpenRouter STT or TTS service:
- **STT** uses Pipecat's OpenAI service pointed at OpenRouter's `/audio/transcriptions`. Each utterance is transcribed
  only after the VAD detects that the caller stopped talking, which adds the upload and transcription time to every
  turn (Deepgram transcribes while the caller speaks). Models: OpenRouter's models API with
  `output_modalities=transcription` (e.g. `openai/whisper-1`, `openai/gpt-4o-mini-transcribe`, `deepgram/nova-3`).
- **TTS** uses a small subclass of Pipecat's OpenAI service in `bot.py`. OpenRouter lists no OpenAI voice model, and
  Pipecat's service only accepts OpenAI voice names, so the subclass accepts any OpenRouter voice model and reads the
  PCM rate from the response's `Content-Type` (`audio/pcm;rate=24000;channels=1`). Models and their voices:
  OpenRouter's models API with `output_modalities=speech` (the `supported_voices` field).
- Measured on 2026-10-28, first audio for one sentence: `elevenlabs/eleven-flash-v2.5` about 0.85–1.1 s,
  `deepgram/flux-tts` 0.8 s, `google/gemini-3.8-flash-tts` 2.4 s, all 24 kHz. Whisper transcribed a 2 s utterance
  in 1.2 s. A phone call to the bench with all three on OpenRouter worked, barge-in included, with roughly 2–3 s
  between the end of the caller's speech and the bot's answer. Cartesia and ElevenLabs direct start faster.

```bash
export BOT_TOKEN=...  # same value as bot_token in FreeSWITCH's vars.xml
export DEEPGRAM_API_KEY=... OPENAI_API_KEY=... CARTESIA_API_KEY=...
export FREESWITCH_ESL_HOST=10.0.0.5 FREESWITCH_ESL_PASSWORD=ClueCon
uv run --with "pipecat-ai[websocket,deepgram,openai,cartesia,silero]" --with python-dotenv --with uvicorn \
  examples/bot.py

# or with ElevenLabs
export TTS_PROVIDER=elevenlabs ELEVENLABS_API_KEY=... ELEVENLABS_VOICE_ID=...
uv run --with "pipecat-ai[websocket,deepgram,openai,elevenlabs,silero]" --with python-dotenv --with uvicorn \
  examples/bot.py

# or with the LLM through OpenRouter
export LLM_PROVIDER=openrouter OPENROUTER_API_KEY=... LLM_MODEL=openai/gpt-4.1-mini
uv run --with "pipecat-ai[websocket,deepgram,openrouter,cartesia,silero]" --with python-dotenv --with uvicorn \
  examples/bot.py
```

Install only the extras of the providers you use. Cartesia and ElevenLabs synthesize at the pipeline output rate,
so with `OUTPUT_RATE` equal to the channel rate in `bot.py` audio reaches FreeSWITCH without resampling. The ElevenLabs
service in Pipecat supports PCM output at 8, 16, 24, 32 and 48 kHz (`pcm_8000` ... `pcm_48000`); this has been
checked in Pipecat's code, but neither provider has been run against its live API here.

Point the dialplan's `uuid_audio_stream` at `ws://BOT_HOST:8765/ws mono 8k ${uuid}`, with `STREAM_EXTRA_HEADERS`
set as in [FreeSWITCH setup](#freeswitch-setup). The bot refuses to start without `BOT_TOKEN` or a service key;
[`.env.example`](.env.example) lists the variables.

**Try it with a phone call, all in Docker:** the bench runs the bot as its `bot` service on extension `9996`. Copy
`.env.example` to `.env` and fill in the keys, run `docker compose --profile bot up -d --build` in
`examples/freeswitch`, register a softphone as `1000` / `1234` on port 5060, and dial `9996` (G.711) or `9995`
(Opus). See [the bench README](examples/freeswitch/README.md#talking-to-the-voice-bot-9996).

The bot defaults to 8 kHz both ways (G.711). A call can ask for other rates in the WebSocket URL, so one bot serves
every codec: for Opus, the dialplan uses `ws://BOT_HOST:8765/ws?in_rate=16000&out_rate=48000` with `mono 16k`
(in the dialplan XML, write the `&` as `&amp;`). `in_rate` must match the `uuid_audio_stream` rate (8000 or 16000)
and `out_rate` should be the channel's codec rate; other values fall back to the defaults with a warning.

## Development

```bash
uv sync --group dev
uv run pytest --cov                    # all tests, with branch coverage (CI requires 95%)
uv run pytest -m "not integration"     # unit tests only
uv run ruff check . && uv run ruff format --check src tests bench examples
```

CI runs the tests on Python 3.12 and 3.13, with both the locked dependencies (the minimum Pipecat supported) and the
latest releases, and checks that the wheel and sdist contain only what they should.

No FreeSWITCH is needed. Unit tests use a fake event socket. The integration tests (`tests/test_integration.py`)
run a real Pipecat pipeline behind `FastAPIWebsocketTransport` on uvicorn, with a WebSocket client playing
mod_audio_stream against the same fake event socket.

[`examples/freeswitch/`](examples/freeswitch/) is a test bench: FreeSWITCH 1.11.3 built from source with
mod_audio_stream, a bench-only SIP profile so FreeSWITCH can call itself, and experiment scripts.

> **The bench is for local testing only.** It uses fixed, public credentials: SIP user `1000` / `1234`, event socket
> password `ClueCon`, WebSocket token `bench-token`. FreeSWITCH runs with host networking and its softphone profile
> listens on the machine's LAN address (port 5060). Run it on a trusted network, never on a host reachable from the
> internet, and do not reuse its configuration in production.

End-to-end test of the serializer with a real Pipecat pipeline:

```bash
cd examples/freeswitch && docker compose up -d && cd ../..
uv sync --group dev
.venv/bin/python bench/tone_bot.py &   # Pipecat bot on extension 9998
uv run pytest -m bench                 # token check, tone + hang-up after speech, and interruption
python3 bench/run_e2e.py               # the same calls, printing every measurement
```

The `bench` tests are deselected by default and skipped when the bench or the bot is not running.

### Releasing

Bump `__version__` in `src/pipecat_freeswitch/__init__.py` (the only place the version is written), move the
CHANGELOG's "Unreleased" entries under the new version, then tag and push: `git tag v0.1.0 && git push origin v0.1.0`.
The `Release` workflow checks that the tag matches the version, runs the tests, builds, and publishes to PyPI through
trusted publishing (one-time setup on pypi.org: see the comment at the top of `.github/workflows/release.yml`).

The bench README documents the phase 0 measurements that shaped the design: why playback uses `sendmsg`, why
`uuid_break` needs `all`, and why hang-up waits for playback.

## Tested versions

Pipecat 1.14.0, FreeSWITCH 1.11.3 (phase 0 results also reproduced on 1.10.12), mod_audio_stream `da8b068`
(community edition), Python 3.12. Codecs tested end to end: PCMU and Opus.

## Contributing

Contributions are welcome: see [CONTRIBUTING.md](CONTRIBUTING.md). Report security vulnerabilities privately,
as described in [SECURITY.md](SECURITY.md).

## License

BSD-2-Clause, same as Pipecat. See [LICENSE](LICENSE).
