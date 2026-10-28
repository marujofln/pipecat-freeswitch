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

### Changed

- `freeswitch_out_sample_rate` defaults to the pipeline output rate when FreeSWITCH can play it, so bot audio is
  not resampled. 64 kHz is no longer accepted.

### Security

- The channel UUID is now validated (canonical 8-4-4-4-12 hex form) before it is used in event socket commands.
  Previously, any client that could reach the bot's WebSocket could send a first message with a newline in the
  "UUID" and run arbitrary ESL commands on FreeSWITCH (e.g. `api system ...`). A malformed `call_uuid` now raises
  `ValueError`; a malformed first message is logged and ignored.

### Fixed

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
