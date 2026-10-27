# pipecat-freeswitch

Community integration that connects [Pipecat](https://github.com/pipecat-ai/pipecat) voice-agent pipelines to
[FreeSWITCH](https://signalwire.com/freeswitch) through the open source
[`mod_audio_stream`](https://github.com/amigniter/mod_audio_stream) WebSocket module.

> **Status: pre-alpha.** Only the phase 0 test bench exists. The serializer, chunker and control client
> (`FreeSWITCHFrameSerializer`, `FreeSWITCHControl`) are not written yet. Nothing here has been validated against
> a live call.

Pipecat's service catalog lists serializers for Twilio, Telnyx, Plivo, Asterisk and others, but none for FreeSWITCH.
This project aims to fill that gap and to serve as the transport layer of an ASR/codec evaluation harness.

## Planned architecture

```
Caller ──SIP/RTP──> FreeSWITCH ──(mod_audio_stream, WebSocket)──> FastAPIWebsocketTransport
                       ▲  │                                           │ FreeSWITCHFrameSerializer
                       │  └─ event mod_audio_stream::play ─┐          ▼
                       │                                   ▼
                       └── ESL: sendmsg execute playback <── FreeSWITCHControl (inbound ESL)
                           uuid_break all, uuid_kill
                                                                    pipeline: VAD → STT → LLM → TTS
```

- **Input** (FreeSWITCH → bot): binary L16 mono, 8 kHz or 16 kHz, packets of `STREAM_BUFFER_SIZE` ms (default 20).
- **Output** (bot → FreeSWITCH): JSON `streamAudio` messages carrying base64 audio blocks. The module writes each block
  to a temporary file and raises the `mod_audio_stream::play` event. It does **not** play the file itself:
  `FreeSWITCHControl` receives the event over ESL and queues `playback <file>` on the channel with `sendmsg`.
  Output is therefore not a stream: audio is grouped into blocks, which trades latency against overhead.
- **Control**: a single inbound ESL connection per call (`filter Unique-ID <uuid>`) handles playback, barge-in
  (`uuid_break <uuid> all`) and hang-up (`uuid_kill`). The ESL client is built on asyncio streams, with no extra
  dependency. `mod_xml_rpc` is not used: it can neither receive events nor send `sendmsg`.
- **Channel UUID**: passed as the last `uuid_audio_stream ... start` argument and delivered as the first text message
  on the WebSocket.

## Repository layout

```
pipecat-freeswitch/
├── bench/
│   ├── echo_server.py      # phase 0 WebSocket echo/tone server + ESL player + control endpoints
│   ├── esl.py              # minimal asyncio ESL client (standard library only)
│   ├── run_experiments.py  # automated phase 0 experiments (SIP/RTP calls to itself, or loopback)
│   ├── rtp_wire.py         # packet capture: RTP on the wire vs what the caller hears
│   ├── requirements.txt
│   └── Dockerfile
├── examples/freeswitch/
│   ├── Dockerfile          # FreeSWITCH built from source + mod_audio_stream
│   ├── docker-compose.yml  # FreeSWITCH + echo server
│   ├── conf/               # minimal FreeSWITCH configuration (dialplan, SIP profile, event socket, ...)
│   └── README.md           # how to run the bench and read the captures
└── docs/                   # research notes (research.md) and specification (specification.md)
```

## Phase 0 test bench

```bash
cd examples/freeswitch
docker compose build   # compiles FreeSWITCH, expect 15-30 minutes
docker compose up
```

Run the automated experiments with `python3 bench/run_experiments.py` (loopback calls, no softphone), or
register a SIP softphone as user `1000`, password `1234`, against the host IP on port 5060, then dial `9999`.
The dialplan runs `uuid_audio_stream` toward `ws://127.0.0.1:8765/ws` (mono, 16 kHz) and parks the call.

The echo server runs in one of two modes (`MODE` environment variable):

| Mode   | Behavior |
|--------|----------|
| `echo` | Sends the caller's audio back in `CHUNK_MS` blocks as `streamAudio`. |
| `tone` | On connect, sends 10 s of a 440 Hz tone in one burst, to observe how the module queues files. |

Per-call events (wall-clock timestamps) are written to `examples/freeswitch/captures/<uuid>.jsonl`.
Experiment endpoints on the echo server (commands go over ESL):

| Endpoint | Action |
|----------|--------|
| `GET /break/<uuid>` | `uuid_break` |
| `GET /break/<uuid>?all=true` | `uuid_break ... all` (does it clear queued files?) |
| `GET /kill/<uuid>` | `uuid_kill` |

### Configuration (echo server)

| Variable | Default | Meaning |
|----------|---------|---------|
| `ECHO_PORT` | `8765` | WebSocket listen port |
| `CHUNK_MS` | `300` | Output block size in milliseconds |
| `OUT_SAMPLE_RATE` | `16000` | Sample rate of generated audio (`tone` mode) |
| `MODE` | `echo` | `echo` or `tone` |
| `PLAY_METHOD` | `sendmsg` | `sendmsg` (ESL execute, gapless) or `broadcast` (`uuid_broadcast`, +100 ms per file) |
| `TONE_SECONDS` | `10` | Tone length |
| `CAPTURE_DIR` | `./captures` | Where logs and raw input audio are written |
| `FS_ESL_HOST`, `FS_ESL_PORT`, `FS_ESL_PASSWORD` | `127.0.0.1`, `8021`, `ClueCon` | FreeSWITCH event socket |

## Findings so far

- The open source `mod_audio_stream` only writes the received audio to `$temp/<uuid>_<n>.tmp.<ext>` and fires the custom
  event `mod_audio_stream::play` (JSON with a `file` field). The bench plays it with an ESL listener that queues
  `playback <file>` on the channel via `sendmsg`. ESL is therefore required for playback, not only for control.
- Supported `streamAudio` types: `raw` (8, 16, 24, 32, 48, 64 kHz), `wav`, `mp3`, `ogg`, `pcmu`, `pcma`.
- Measured on the bench (details in [`examples/freeswitch/README.md`](examples/freeswitch/README.md)):
  - `uuid_broadcast` adds 100 ms of silence before every file (hardcoded `lead-frames: 5` in
    FreeSWITCH), so 100 ms blocks play at half speed. Queuing `playback` through ESL `sendmsg` instead gives
    gapless playback and first audio 24-41 ms after `streamAudio` is sent, for blocks from 100 to 1000 ms.
  - `uuid_break` alone stops only the current file; `uuid_break <uuid> all` clears the queue (22 ms).
  - `uuid_kill` hangs up in about 20 ms, but killing right after the last block cuts it off (240 ms with 200 ms
    blocks); waiting for the last file's `CHANNEL_EXECUTE_COMPLETE` keeps the whole speech.
  - With blocks sent in real time and up to 50 ms of jitter, playback underruns are rare (0-1 per call, ~20 ms);
    a one-block prebuffer removes them.
  - The bench's receiving leg loses ~40 ms at the start of each talkspurt over SIP; a packet capture shows the RTP
    sent toward the caller is complete, so this is receiver behavior, not the package's.

## Roadmap

- **Phase 0**: bench and measurements (done).
- **v0.1**: `FreeSWITCHFrameSerializer`, chunker, `FreeSWITCHControl`, single-file example, tests, PyPI release.
- **v0.2**: DTMF support, adapters for `mod_audio_fork` and `mod_earshot`.
- **v0.3**: latency metrics (time to first audio by block size).

## Tested versions

Pipecat: not yet tested (target ≥ 1.10.0). FreeSWITCH: v1.11.3 (bench image; phase 0 results also
reproduced on v1.10.12). mod_audio_stream: `da8b068` (community edition). Python: 3.12+.

## License

BSD-2-Clause, same as Pipecat. See [LICENSE](LICENSE).
