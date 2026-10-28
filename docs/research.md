# Research — Pipecat, FreeSWITCH and mod_audio_stream

Background research done on 2026-10-27, when the project started. Star counts and versions are as of that date.

## 1. Pipecat

- Repo: https://github.com/pipecat-ai/pipecat — BSD-2, ~16.8k stars, maintained by Daily.
- v1.0.0 on 2026-04-14; v1.3.0 (2026-05-29) added a multi-agent framework.
- Model: a pipeline of processors exchanging frames (audio, text, control). STT → LLM → TTS with VAD (Silero),
  Smart Turn, interruptions and end-to-end streaming.
- Ecosystem: Pipecat Flows (state machine), Whisker (debugger), Tail (terminal dashboard), `pipecat init` CLI,
  `pipecat eval run` eval harness, Pipecat Cloud.
- Known criticism: deploying at scale is left to the user; commercial gravity pulls toward Daily WebRTC /
  Pipecat Cloud.

## 2. FreeSWITCH ↔ Pipecat landscape

- Service catalog: https://docs.pipecat.ai/api-reference/server/services/supported-services
  Serializers: Acefone, Asterisk (community), AwaazAI, Bandwidth, didlogic, Exotel, Genesys, Plivo, Telnyx,
  Twilio, Vonage, Wavix. **No FreeSWITCH.**
- `pipecat-asterisk` (the model to follow): https://github.com/NikolayShakin/pipecat-asterisk
  - `AsteriskFrameSerializer` + `AsteriskWebsocketTransport` with flow control (low/high water marks).
  - Works with `slin`; recommends matching the pipeline sample rate to the channel's to avoid double resampling.
  - Example with an Asterisk docker-compose and extensions per sample rate; quality test script per rate.
  - Tested with Pipecat v1.1.0, Python 3.12+.
- Prior art on the FreeSWITCH side:
  - `mod_earshot` (https://github.com/wiringai/mod_earshot): full-duplex module with several protocols, including
    Pipecat via protobuf; latency metrics and typed ESL events. Claims `mod_audio_stream` playback is unreliable.
  - `aplisay/llm-agent` has an internal `FreeSwitchAudioStreamSerializer` (not packaged):
    https://github.com/aplisay/llm-agent/pull/325
- `mod_audio_stream` forks: amigniter (original), rasonyang, saravanansaru, connexcs. The commercial edition with
  bidirectional binary streaming is free up to 10 channels, with no source code.

## 3. mod_audio_stream protocol (open source)

- `uuid_audio_stream <uuid> start <ws-url> <mono|mixed|stereo> <8k|16k> [metadata]` — sends L16; the metadata is
  sent before the audio.
- `uuid_audio_stream <uuid> send_text|stop|pause|resume`.
- Channel variable `STREAM_BUFFER_SIZE` (ms, multiple of 20) sets the size of the packets sent.
- Events: `mod_audio_stream::json|connect|disconnect|error|play`.
- Playback: the server sends
  `{"type":"streamAudio","data":{"audioDataType":"raw|wav|mp3|ogg","sampleRate":8000,"audioData":"<base64>"}}`;
  the module saves it to a temporary file and raises `mod_audio_stream::play` with the path. (Phase 0 showed the
  module does not play the file itself; see `docs/specification.md`.)
- Source: https://github.com/connexcs/mod_audio_stream (README identical to amigniter's).

## 4. Requirements for listing as a community integration

Source: https://github.com/pipecat-ai/pipecat/blob/main/COMMUNITY_INTEGRATIONS.md
- Repo with code, a foundational single-file example, README (intro, installation, pipeline usage, how to run the
  example, tested Pipecat version), permissive LICENSE (BSD-2), docstrings, CHANGELOG.
- Name `pipecat-{vendor}`; class `VendorFrameSerializer`.
- Telephony: hang up through the native API (ideally `aiohttp`); DTMF → `InputDTMFFrame` with `KeypadEntry`,
  invalid → `None`.
- A serializer is not a FrameProcessor: actions on `EndFrame`/`CancelFrame` only through the frame path.
- Errors: `push_error` + exception. Tests: the example as an integration test, `pipecat eval run`, optional unit tests.
- Docs: fork https://github.com/pipecat-ai/docs, a row in `supported-services.mdx`, a page at
  `api-reference/server/services/serializers/freeswitch.mdx` with the `CommunityMaintained` badge, an entry in
  `docs.json`.
- PR with a 30–60 s video showing it working and one interruption; announce it in the Discord
  `#community-integrations` channel.

