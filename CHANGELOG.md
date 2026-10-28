# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `output_gain_db` (default 0): gain applied to bot audio before it is sent, clipped to 16 bits. TTS speech measured
  about 12 dB above telephone level, with full-scale peaks, and distorted on a desk phone and a softphone.
- `freeswitch_in_sample_rate` and `freeswitch_out_sample_rate`: separate stream rates per direction, e.g. 16 kHz
  caller audio and 48 kHz bot audio on Opus channels.
- Bot audio at 48 kHz is sent as WAV (FreeSWITCH plays raw audio only at 8, 16, 24 and 32 kHz).
- `output_resampler_quality` (default "QQ").
- `examples/bot.py` can use ElevenLabs as well as Cartesia: set `TTS_PROVIDER=elevenlabs`, `ELEVENLABS_API_KEY`
  and `ELEVENLABS_VOICE_ID`. Cartesia stays the default; its voice can be set with `CARTESIA_VOICE_ID`.
- Tests for the error paths of the event socket client and the control (failed commands, dropped connections,
  malformed events), and in-process integration tests that run a real Pipecat pipeline behind
  `FastAPIWebsocketTransport` with a client playing mod_audio_stream. CI enforces 95% branch coverage.
- `pytest -m bench` runs the end-to-end calls of `bench/run_e2e.py` against the FreeSWITCH bench and checks their
  results; deselected by default and skipped when the bench is not running.
- CI: tests on Python 3.12 and 3.13, each with the locked and the latest dependencies; lint of `bench/` and
  `examples/` too (the bench at 120 columns); a check of the wheel and sdist contents.
- Release workflow: publishes to PyPI through trusted publishing when a `v*` tag matching the version is pushed.
- The package ships `py.typed`. Python 3.13 classifier.
- A warning, once per process, when the event socket is used with FreeSWITCH's default password ("ClueCon").
- The example voice bot runs in Docker: `examples/bot.Dockerfile` and a `bot` service in the bench's compose file
  (profile `bot`, extension `9996`, keys from the repository's `.env`), so a softphone call reaches the full
  STT → LLM → TTS pipeline with one `docker compose` command. `.env.example` lists the variables.
- `examples/bot.py` takes the caller and bot audio rates per call from the WebSocket URL (`?in_rate=16000&out_rate=48000`),
  with 8 kHz both ways as the default, so one bot serves G.711 and Opus calls. The bench adds extension `9995`
  (Opus: 16 kHz stream, 48 kHz bot audio) next to `9996` (G.711), and its softphone profile now accepts Opus as
  well as PCMU and PCMA.
- `examples/bot.py` checks the service keys at startup instead of failing in the first call, and reads `HOST`
  (default `0.0.0.0`) as well as `PORT`.
- `examples/bot.py` can use OpenRouter for all three services, with one `OPENROUTER_API_KEY`: `LLM_PROVIDER=openrouter`
  (Pipecat's `OpenRouterLLMService`, any model via `LLM_MODEL`), `STT_PROVIDER=openrouter` (Pipecat's OpenAI STT
  pointed at `/audio/transcriptions`, default `openai/whisper-1`) and `TTS_PROVIDER=openrouter` (a subclass of
  Pipecat's OpenAI TTS that accepts any OpenRouter voice model and reads the PCM rate from the response, default
  `elevenlabs/eleven-flash-v2.5` with voice `sarah`). Both speech services work request by request. Tested with a
  phone call to the bench, barge-in included. `STT_PROVIDER`, `LLM_PROVIDER` and `LLM_MAX_TOKENS` (default 300) are
  new; `LLM_MODEL` replaces the hard-coded model.

### Documentation

- `docs/specification.md` revised after the project review: parameters and rate defaults, UUID validation and
  WebSocket token, ending the call (`cleanup()`, connection closed on hang-up), file-name fallback, tests layout.
- README and specification: mod_audio_stream keeps every block file in the temp directory until the call's stream
  ends (1,500 files per minute of bot speech with 40 ms blocks); size the temp directory and clean up after crashes.

### Changed

- `freeswitch_out_sample_rate` defaults to `freeswitch_sample_rate` (8000) and is validated when the serializer is
  built. Bot audio at another pipeline rate (e.g. a 24 kHz TTS) is resampled by the serializer to the channel rate,
  rather than sent as is and resampled by FreeSWITCH block by block, with an artefact at every seam. Opus channels
  need `freeswitch_out_sample_rate=48000`. 64 kHz is no longer accepted.

### Security

- The channel UUID is now validated (canonical 8-4-4-4-12 hex form) before it is used in event socket commands.
  Previously, any client that could reach the bot's WebSocket could send a first message with a newline in the
  "UUID" and run arbitrary ESL commands on FreeSWITCH (e.g. `api system ...`). A malformed `call_uuid` now raises
  `ValueError`; a malformed first message is logged and ignored.
- `examples/bot.py` and the bench bot require a bearer token on the WebSocket handshake, sent by FreeSWITCH through
  `STREAM_EXTRA_HEADERS` (`bot_token` in vars.xml), so a client that can reach the WebSocket cannot name another
  call's UUID and interrupt or hang it up. The example bot needs `BOT_TOKEN`. The README documents the setup and
  the need for `wss://` or a private network. `bench/run_e2e.py` gains an `auth` scenario.
- Bench echo server (`bench/echo_server.py`): it listened on all interfaces (with `network_mode: host`) and passed
  the UUID from the URL of `/kill/{uuid}` and `/break/{uuid}`, and from the first WebSocket message, unchecked into
  ESL commands and capture file names. Anyone who could reach port 8765 while the bench ran could run ESL commands
  on the bench FreeSWITCH or append to files outside the captures directory. It now listens on `127.0.0.1`
  (`ECHO_HOST` to change it), answers HTTP 400 to a non-canonical UUID and closes such a WebSocket with 1008.
  `bench/tone_bot.py` validates the UUID too, since it names the capture file.

### Fixed

- **Barge-in by voice never worked with the example's 40 ms blocks.** Each FreeSWITCH `playback` sends an audio sync
  that resets the jitter buffer and flushes the caller's incoming audio, costing 30–40 ms of caller audio per block:
  with 40 ms blocks the bot heard nothing while it spoke (~100% lost, measured with a continuous-tone caller), so it
  was only interrupted when a late transcription arrived. `examples/bot.py` now sends 200 ms blocks (`BLOCK_MS`; 14%
  lost) and the README documents the trade-off. A growing-block chunker is planned.
- `examples/bot.py` defaults to `OUTPUT_GAIN_DB=-12` (bot speech at −24.7 dBFS RMS, peaks −12.8 dBFS, instead of
  −13 dBFS with full-scale peaks) and `VAD_MIN_VOLUME=0.4` (Pipecat's 0.6 missed quiet phone speech); `VAD_CONFIDENCE`
  is configurable too.
- Bench: the voice-bot extensions pass the channel's negotiated rate (`out_rate=${write_rate}`). Opus ran at 16 kHz on
  a Grandstream GXV3275, and sending 48 kHz made FreeSWITCH resample every block, with a click at each seam.
- Bot audio held while the channel UUID is unknown could grow without bound; only the last 5 s are kept now, with
  a warning. `FreeSWITCHControl.stop()` awaits the event task it cancels.
- The version is written only in `pipecat_freeswitch.__version__`; `pyproject.toml` reads it from there.

- The sdist picked up local working files: 315 bench captures (3 MB of WAV and JSONL, ignored by git through
  `examples/freeswitch/captures/.gitignore`, which hatch does not read) and `CLAUDE.md`. It now lists its contents
  explicitly: the package, the tests, `examples/bot.py`, README, CHANGELOG and LICENSE (30 KB).

- A WebSocket that closed before the pipeline ended (caller hang-up, network drop) left the event socket connection
  open and, if the call was still up, the call parked: the transport stops passing frames to the serializer once
  the WebSocket is closed, so `CancelFrame` never reached it. New `FreeSWITCHFrameSerializer.cleanup()` hangs up
  and closes the connection; call it from `on_client_disconnected` (the example and bench bots do). The control
  also closes its connection on `CHANNEL_HANGUP_COMPLETE`, so it never outlives the call.

- A `mod_audio_stream::play` file whose name did not match `<uuid>_<n>.tmp.<ext>` was played but never counted, so
  hang-up on `EndFrame` waited for `hang_up_timeout_secs`. Such files are now numbered in arrival order (with a
  warning), which also keeps dropping stale audio after an interruption.

- Bot audio resampled by the serializer lost the end of each utterance: the default SOXR "VHQ" stream resampler
  holds back up to ~110 ms, which nothing flushes, and clears it after 0.2 s of inactivity. Output resampling now
  uses "QQ", which holds back nothing.

## [0.1.0] - 2026-10-27

### Added

- `FreeSWITCHFrameSerializer` for Pipecat's `FastAPIWebsocketTransport` and the open source FreeSWITCH
  `mod_audio_stream`: caller L16 audio to `InputAudioRawFrame`, bot audio to `streamAudio` messages (one per output
  frame), channel UUID from `call_uuid` or the first WebSocket message.
- `FreeSWITCHControl`: one inbound event socket connection per call that plays the files written by
  mod_audio_stream with `sendmsg` (no `lead-frames` gap), interrupts with `uuid_break all` and drops audio sent
  before the interruption, and hangs up with `uuid_kill` after the queued audio has played (`EndFrame`) or at once
  (`CancelFrame`).
- `ESLConnection`: minimal asyncio event socket client with no extra dependency.
- `examples/bot.py`: Deepgram + OpenAI + Cartesia voice bot.
- Unit tests with a fake event socket; end-to-end test bench (`examples/freeswitch/`, `bench/tone_bot.py`,
  `bench/run_e2e.py`).
