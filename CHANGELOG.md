# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `freeswitch_in_sample_rate` and `freeswitch_out_sample_rate`: separate stream rates per direction, e.g. 16 kHz
  caller audio and 48 kHz bot audio on Opus channels.
- Bot audio at 48 kHz is sent as WAV (FreeSWITCH plays raw audio only at 8, 16, 24 and 32 kHz).
- `output_resampler_quality` (default "QQ").
- `examples/bot.py` can use ElevenLabs as well as Cartesia: set `TTS_PROVIDER=elevenlabs`, `ELEVENLABS_API_KEY`
  and `ELEVENLABS_VOICE_ID`. Cartesia stays the default; its voice can be set with `CARTESIA_VOICE_ID`.
- Tests for the error paths of the event socket client and the control (failed commands, dropped connections,
  malformed events), and in-process integration tests that run a real Pipecat pipeline behind
  `FastAPIWebsocketTransport` with a client playing mod_audio_stream. CI enforces 95% branch coverage.

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
