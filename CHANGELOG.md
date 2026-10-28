# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-10-28

First public release.

### Added

- `FreeSWITCHFrameSerializer` for Pipecat's `FastAPIWebsocketTransport` and the open source FreeSWITCH
  `mod_audio_stream` (community edition): caller L16 audio to `InputAudioRawFrame`, bot audio to `streamAudio`
  messages, channel UUID from `call_uuid` or from the first WebSocket message.
- `FreeSWITCHControl`: one inbound event socket (ESL) connection per call that plays the files written by
  mod_audio_stream with `sendmsg` (no `lead-frames` gap, unlike `uuid_broadcast`), interrupts with
  `uuid_break all` and drops audio sent before the interruption, and hangs up with `uuid_kill` after the queued
  audio has played (`EndFrame`) or at once (`CancelFrame`). It closes its connection when the channel hangs up.
- `FreeSWITCHFrameSerializer.cleanup()`: hangs up (if `auto_hang_up`) and closes the event socket. Call it from
  `on_client_disconnected`: once the WebSocket is closed, Pipecat's transport no longer passes `CancelFrame` to the
  serializer (reported upstream as [pipecat-ai/pipecat#6440](https://github.com/pipecat-ai/pipecat/issues/6440)).
- `ESLConnection`: minimal asyncio event socket client with no extra dependency.
- Parameters: `freeswitch_sample_rate`, `freeswitch_in_sample_rate`, `freeswitch_out_sample_rate` (8, 16, 24 and
  32 kHz as raw audio, 48 kHz as WAV), `output_resampler_quality` (default "QQ", which holds no audio back),
  `output_gain_db`, `sample_rate`, `auto_hang_up`, `hang_up_timeout_secs`.
- A warning, once per process, when the event socket is used with FreeSWITCH's default password ("ClueCon").
- `examples/bot.py`: a voice bot with selectable services (`STT_PROVIDER`: Deepgram or OpenRouter; `LLM_PROVIDER`:
  OpenAI or OpenRouter; `TTS_PROVIDER`: Cartesia, ElevenLabs or OpenRouter), per-call audio rates from the WebSocket
  URL, a bearer token on the handshake, service keys checked at startup, and tuned defaults for phone audio
  (`BLOCK_MS=200`, `OUTPUT_GAIN_DB=-12`, `VAD_MIN_VOLUME=0.4`). `.env.example` lists the variables.
- A FreeSWITCH 1.11.3 test bench in Docker (`examples/freeswitch/`): mod_audio_stream, a test bot, an echo server,
  and the example bot as a `bot` service on extensions `9996` (8 kHz) and `9995` (16 kHz stream); experiment
  scripts in `bench/`.
- Tests: unit tests with a fake event socket, in-process integration tests with a real Pipecat pipeline behind
  `FastAPIWebsocketTransport`, and `pytest -m bench` for end-to-end calls through the bench. CI on Python 3.12 and
  3.13 with locked and latest dependencies, 95% branch coverage, and a check of the wheel and sdist contents.
- PyPI release through trusted publishing on a `v*` tag; the package ships `py.typed`.

### Security

- The channel UUID must be canonical (8-4-4-4-12 hex) before it is used in event socket commands, so the WebSocket's
  first message cannot inject ESL commands.
- The example and bench bots require a bearer token on the WebSocket handshake, sent by FreeSWITCH through
  `STREAM_EXTRA_HEADERS`. `SECURITY.md` explains how to report vulnerabilities and the deployment measures this
  relies on.

### Known limitations

- Each FreeSWITCH `playback` flushes 30–40 ms of the caller's incoming audio, so blocks must be 200 ms or more for
  barge-in by voice to work (14–16% of the caller's audio is still lost while the bot speaks). A growing-block chunker
  is planned.
- No DTMF yet (planned for 0.2).
- mod_audio_stream keeps every block file in FreeSWITCH's temp directory until the call's stream ends.
