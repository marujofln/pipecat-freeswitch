# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

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
