# Specification — pipecat-freeswitch (draft v2)

Technical design defined in the 2026-10-27 session and revised the same day with the Phase 0 results
(bench in `examples/freeswitch/`, numbers in `examples/freeswitch/README.md`).

## Goal

Let a Pipecat bot answer and place calls on FreeSWITCH using the open source `mod_audio_stream`, with working
barge-in and hang-up, packaged as a community integration listed in the Pipecat docs.

## Architecture

```
Caller ──SIP/RTP──> FreeSWITCH ──(mod_audio_stream, WebSocket)──> FastAPIWebsocketTransport
                       ▲   │                                          │ FreeSWITCHFrameSerializer
                       │   └─ event mod_audio_stream::play ─┐         ▼
                       │                                    ▼
                       └──── ESL: sendmsg execute playback <── FreeSWITCHControl (inbound ESL)
                             uuid_break all / uuid_kill
                                                              pipeline: VAD → STT → LLM → TTS
```

- **Input** (FreeSWITCH → bot): binary L16 mono, 8 kHz or 16 kHz, packets of `STREAM_BUFFER_SIZE` ms (default 20).
  Measured: 640 bytes per packet at 16 kHz, mean interval 19.9 ms, p95 41.5 ms (packets sometimes arrive in pairs).
- **Output** (bot → FreeSWITCH): JSON `streamAudio` messages carrying base64 audio blocks. The module **only writes**
  each block to `$temp/<uuid>_<n>.tmp.r16` and raises the `mod_audio_stream::play` event (JSON with a `file` field).
  **It plays nothing.** `FreeSWITCHControl` receives the event over ESL and queues `playback <file>` on the channel.
- **Playback: use ESL `sendmsg`, not `uuid_broadcast`.** `uuid_broadcast` adds a hardcoded `lead-frames: 5` to every
  command (`src/switch_ivr_async.c:5737` in FreeSWITCH 1.11.3; line 5736 in 1.10.12), and the channel discards
  5 frames before each file (`src/switch_ivr.c:540-554`): 100 ms of silence per file at a 20 ms ptime. With `sendmsg`
  (`call-command: execute`, `execute-app-name: playback`, `event-lock: true`) back-to-back files are gapless and
  first audio comes out 24–41 ms after `streamAudio` is sent (1.11.3).
- **Control:** `uuid_break <uuid> all` for barge-in (without `all` only the current file stops and the rest of the
  queue keeps playing) and `uuid_kill <uuid>` for hang-up, over the same ESL connection.

### Decision: control channel = inbound ESL for everything (2026-10-27)

| Function | Needs | ESL | `mod_xml_rpc` |
|---|---|:-:|:-:|
| Receive `mod_audio_stream::play` | event subscription | yes | no (request/response only) |
| Gapless playback | `sendmsg execute` (no `api` command does it without `lead-frames`) | yes | no |
| Know when the last file finished | `CHANNEL_EXECUTE_COMPLETE` | yes | no |
| `uuid_break all`, `uuid_kill` | `api` command | yes | yes |

ESL is required for playback, and `mod_xml_rpc` would only cover break/kill, at the cost of one more module, port
and credential. The Pipecat guide's recommendation ("hang up through the native API, ideally with `aiohttp`") targets
REST providers; on FreeSWITCH the native control API is ESL.

- One ESL connection per call: opened in the serializer's `setup()`, `filter Unique-ID <uuid>` as soon as the UUID
  arrives, closed when the call ends.
- Own client on plain asyncio (based on `bench/esl.py`), no new dependency.
- Security: ESL amounts to full control of FreeSWITCH (as does `mod_xml_rpc`'s `/webapi`); require a private
  network, an ACL and a dedicated password, and document it.
- v0.2: outbound ESL (`socket bot:8084 async full` in the dialplan) as a deployment alternative that does not expose
  port 8021.

## Repository layout

```
pipecat-freeswitch/
├── src/pipecat_freeswitch/
│   ├── __init__.py
│   ├── serializer.py       # FreeSWITCHFrameSerializer
│   ├── control.py          # FreeSWITCHControl: ESL (play via sendmsg, uuid_break all, uuid_kill)
│   └── chunker.py          # groups output audio into streamAudio blocks
├── bench/                  # Phase 0 bench: echo_server.py, esl.py, run_experiments.py, rtp_wire.py
├── examples/
│   ├── bot.py              # foundational single-file example
│   └── freeswitch/
│       ├── Dockerfile          # FreeSWITCH 1.11.3 built from source + mod_audio_stream + mod_loopback
│       ├── docker-compose.yml  # FreeSWITCH + bench server (later: + bot)
│       └── conf/               # dialplan, SIP profiles, modules.conf.xml, event_socket.conf.xml
├── tests/
│   ├── test_serializer.py
│   └── fixtures/           # L16 8k/16k samples
├── .github/workflows/      # lint, tests, PyPI release
├── CHANGELOG.md
├── LICENSE                 # BSD-2
├── README.md
└── pyproject.toml          # uv
```

## FreeSWITCHFrameSerializer (sketch)

```python
class FreeSWITCHFrameSerializer(FrameSerializer):
    """Serializer for FreeSWITCH mod_audio_stream (open source edition)."""

    # params: sample_rate (8000|16000), chunk_ms, auto_hang_up=True,
    #         control: FreeSWITCHControl, optional call_uuid (otherwise taken from the first text message)

    # deserialize: FreeSWITCH -> Pipecat
    #   first text message -> metadata (channel UUID) -> control.attach(uuid) starts playing files
    #   binary L16         -> InputAudioRawFrame (resample if rate != pipeline)
    #   JSON event         -> log; DTMF -> InputDTMFFrame (v0.2)
    #
    # serialize: Pipecat -> FreeSWITCH
    #   OutputAudioRawFrame  -> chunker -> {"type":"streamAudio","data":{"audioDataType":"raw","sampleRate":N,"audioData":b64}}
    #   InterruptionFrame    -> drop the chunker buffer + control.uuid_break(uuid, all=True)
    #   EndFrame/CancelFrame -> flush/drop + control.uuid_kill(uuid) if auto_hang_up
```

```python
class FreeSWITCHControl:
    """ESL client: plays files announced by mod_audio_stream and sends break/kill."""

    # connect(host, port, password); filter Unique-ID <uuid>;
    # subscribe CUSTOM mod_audio_stream::play CHANNEL_EXECUTE_COMPLETE
    # on play event for an attached uuid -> sendmsg <uuid> execute playback <file> (no lead-frames)
    # interrupt(uuid) -> api uuid_break <uuid> all
    # hang_up(uuid)   -> wait for the last file's CHANNEL_EXECUTE_COMPLETE, then api uuid_kill <uuid>
```

Points of attention:
- **Block size:** with `sendmsg` there is no fixed cost per file, so blocks can be small (100 ms measured with no
  gap). What remains is the latency × overhead trade-off (base64, one file and one event per block), which is the
  open experimental question. Re-check with 20–60 ms blocks.
- **Pacing (PR pipecat-ai/pipecat#5593, fixed in Pipecat 1.10.0):** if the serializer emits no payload for an audio
  frame, the transport skips pacing and `BotStoppedSpeakingFrame`/`EndFrame` arrive early. Since the chunker
  accumulates frames (returning `None` for many of them), verify this behavior and require Pipecat ≥ 1.10.0.
- **Flush** the last partial block when the bot stops speaking (e.g. on `TTSStoppedFrame`).
- **File order:** `play` events arrive in `streamAudio` order, and `sendmsg` keeps the channel queue in order.
  `uuid_break all` clears the queue, but `play` events in flight can arrive after the break. Fix: the module names
  files `<uuid>_<n>` with `n` increasing per `streamAudio` (`audio_streamer_glue.cpp:430`); the serializer counts
  the `streamAudio` messages it sent and, on a break, drops `play` events whose `n` is below that count. This relies
  on a module internal (regression test + pinned version); a message that fails to decode does not increment `n`.
- **Playback queue running dry with real-time sending (measured over SIP, 2026-10-27).** With 40–200 ms blocks and up
  to 50 ms of random delay: 0–1 underrun per call, ~20 ms each. The queue self-heals (after one late block,
  playback runs that much later and gains margin), so total silence is bounded by the largest delay in the call.
  A one-block prebuffer removes almost all of them (it only fails when the delay exceeds the block size), at the
  cost of one block of latency. Decision for the chunker: configurable one-block prebuffer, on by default.
- **`uuid_kill` on `EndFrame` cuts off the end of the speech (confirmed).** Kill right after the last block: the last
  file never started and 240 ms of speech were lost (200 ms blocks). Waiting for the last file's
  `CHANNEL_EXECUTE_COMPLETE`: nothing lost, hang-up 270 ms after the last block. The control must wait (with a
  timeout).
- **~40 ms lost at the start of each talkspurt (SIP): on the receiving side, not the sending side.** The bench's
  caller leg (a FreeSWITCH receiving) loses ~40 ms whenever audio resumes after silence. The packet capture
  (`bench/rtp_wire.py`) shows the RTP sent by the bot leg is complete, with no sequence or timestamp gaps; the only
  gap on the wire is the real ~20 ms underrun. It does not affect the package; it would only matter if the bot's
  FreeSWITCH sent to another FreeSWITCH, and then it would be that receiver's behavior.
- **Bench with `mod_loopback`:** loses the last 20 ms frame of each back-to-back file (`playback` sends an audio sync
  when it starts and the loopback clears its outbound queue). It is a loopback artifact; audio measurements must use
  SIP (`--endpoint sip`, the default).
- **Early `BotStoppedSpeakingFrame`:** it arrives up to one block before the audio really ends. Small effect on turn
  detection; document it.
- **Hang-up:** `uuid_kill` tears the channel down in ~20 ms and the WebSocket closes in ~15 ms. The module does not
  raise `mod_audio_stream::disconnect` on hang-up, only when the server closes the connection.
- **Deployment:** the bench's ESL listens on `127.0.0.1` only. In production the bot needs access to the event socket
  (ACL + password); document it in the README.
- **ptime:** measurements used 20 ms. With another ptime the `uuid_broadcast` gap would change (5 × ptime); with
  `sendmsg` it should not matter, but check it in the harness with different codecs.

## Test dialplan (validated on the bench)

```xml
<extension name="pipecat_bench">
  <condition field="destination_number" expression="^9999$">
    <action application="answer"/>
    <action application="set" data="STREAM_BUFFER_SIZE=20"/>
    <action application="set"
      data="stream_result=${uuid_audio_stream(${uuid} start ws://127.0.0.1:8765/ws mono 16k ${uuid})}"/>
    <action application="park"/>
  </condition>
</extension>
```

The earlier sketch did not pass `<uuid>` as the first argument of `uuid_audio_stream`.

## Roadmap

- **Phase 0 — bench (done on 2026-10-27):** docker-compose with FreeSWITCH 1.11.3 built from source +
  `mod_audio_stream` (amigniter, commit `da8b068`) + `mod_event_socket` + `mod_loopback`; FastAPI echo server;
  `bench/run_experiments.py` with SIP/RTP calls to itself (or loopback); `bench/rtp_wire.py` packet capture.
  Answered: L16 arrival, latency per block size, source of the 100 ms gap (`uuid_broadcast`'s `lead-frames`),
  `uuid_break` with and without `all`, `uuid_kill`, real-time pacing, hang-up after speech.
- **v0.1:** serializer + chunker + control (ESL), `bot.py` example, docker-compose, README, CHANGELOG, serializer
  tests, 30–60 s video with an interruption, PR to the Pipecat docs. Enough for listing.
- **v0.2:** DTMF (ESL or a dialplan bind), adapters for `mod_audio_fork` and `mod_earshot`'s native protocol.
- **v0.3:** latency metrics (time to first audio per block size).

## Listing checklist (community integrations guide)

- [ ] Code with Pipecat-style docstrings
- [ ] Foundational single-file example
- [ ] README: intro, installation, usage, how to run the example, tested Pipecat version
- [x] BSD-2 LICENSE
- [ ] CHANGELOG, PyPI release
- [ ] Hang-up through the native API (ESL); DTMF → `InputDTMFFrame` (if supported)
- [ ] Fork of pipecat-ai/docs: row in `supported-services.mdx`, page `serializers/freeswitch.mdx`, `docs.json`
- [ ] 30–60 s video showing it working + an interruption
- [ ] Post in the Discord `#community-integrations` channel
