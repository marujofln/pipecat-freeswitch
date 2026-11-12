# Specification — pipecat-freeswitch (v0.1)

Technical design defined in the 2026-10-27 session, revised the same day with the Phase 0 results
(bench in `examples/freeswitch/`, numbers in `examples/freeswitch/README.md`) and implemented in v0.1.0.
Revised on 2026-10-28 after the project review: UUID validation and WebSocket token, output rate default,
file-name fallback, `cleanup()`, temp files.

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
  arrives. It is closed on `EndFrame`/`CancelFrame`, by `serializer.cleanup()` when the WebSocket closes first, and by
  the control itself on `CHANNEL_HANGUP_COMPLETE`, so it never outlives the call (see
  [Ending the call](#ending-the-call-2026-10-28)).
- Own client on plain asyncio (based on `bench/esl.py`), no new dependency.
- Security: ESL amounts to full control of FreeSWITCH (as does `mod_xml_rpc`'s `/webapi`); require a private
  network, an ACL and a dedicated password, and document it.
- v0.2: outbound ESL (`socket bot:8084 async full` in the dialplan) as a deployment alternative that does not expose
  port 8021.

### Security of the WebSocket (2026-10-28)

The first WebSocket message names the channel the serializer controls, and that name goes into ESL commands.

- **UUID validation.** `FreeSWITCHControl.start()` and the serializer accept only canonical UUIDs (8-4-4-4-12 hex,
  `control.is_valid_uuid`). Before this, a newline in the "UUID" started another ESL command (e.g. `api system ...`):
  remote command execution for anyone who could reach the WebSocket. A malformed `call_uuid` raises `ValueError`; a
  malformed first message is logged and ignored. Trade-off: custom `origination_uuid` values in another format are
  rejected.
- **Bearer token on the handshake.** Validation does not stop a client from naming another call's valid UUID. So
  FreeSWITCH sends `Authorization: Bearer <bot_token>` through mod_audio_stream's `STREAM_EXTRA_HEADERS` (a JSON
  object of handshake headers; the module's libwsc client also keeps a URL's query string, but a header stays out of
  logs). The bot checks it before `accept()`, with `secrets.compare_digest`, and closes with 1008 (HTTP 403)
  otherwise. This lives in the application (`examples/bot.py`), not in the package: the serializer never sees the
  HTTP request. The README documents generation, configuration and rotation.
- **Limits.** Over `ws://` the token travels in clear text (use `wss://`, `STREAM_TLS_*`, or a private network). A
  single shared token lets its holder name any call; a per-call token (HMAC of the UUID) is a possible v0.2 item.
- The bench applies the same rules: `bench/tone_bot.py` requires the token (`bench-token`), and
  `bench/echo_server.py` listens on `127.0.0.1` and rejects non-canonical UUIDs.

### DTMF (2026-11-03)

FreeSWITCH reports each key as a `DTMF` event (`DTMF-Digit`) on the channel, which only the event socket sees. The
serializer can only hand frames to the pipeline from `deserialize()`, which the transport calls for WebSocket
messages, so the key has to come in through the WebSocket:

- `FreeSWITCHControl` subscribes to `DTMF` with the other events and, for each key, runs
  `api uuid_audio_stream <uuid> send_text {"event":"dtmf","digit":"5"}`. mod_audio_stream (`da8b068`) sends that
  text on the call's WebSocket as a text message. It splits API arguments on spaces with `switch_separate_string`,
  which strips single quotes and backslash escapes but keeps double quotes, so the JSON is written without spaces.
- The digit goes into an event socket command: only one character of `0-9*#A-D` is forwarded, anything else is
  logged and dropped.
- `deserialize()` turns `{"event":"dtmf","digit":...}` text messages (after the channel UUID) into
  `InputDTMFFrame`. A–D have no `KeypadEntry` and are dropped.
- Alternatives rejected: a dialplan `bind_digit_action` still needs a way into the pipeline, and returning queued
  keys from the next audio `deserialize()` call would drop or delay caller audio.
- The WebSocket client could send the same message itself. It is already authenticated by the bearer token and owns
  the call's audio, so this grants nothing new.
- Bench: `uuid_send_dtmf` on the SIP caller leg (RFC 2833) delivered `1234567890*#` in order to the bot's pipeline.
  In-band tones need `start_dtmf` in the dialplan (not tested).

## Repository layout

```
pipecat-freeswitch/
├── src/pipecat_freeswitch/
│   ├── __init__.py
│   ├── serializer.py       # FreeSWITCHFrameSerializer
│   ├── lookahead.py        # FreeSWITCHLookahead: hands bot audio to the serializer ahead of the transport
│   ├── control.py          # FreeSWITCHControl: ESL (play via sendmsg, uuid_break all, uuid_kill)
│   └── esl.py              # ESLConnection: minimal asyncio event socket client
├── bench/                  # bench: echo_server.py, esl.py, run_experiments.py, rtp_wire.py,
│                           #        tone_bot.py + run_e2e.py (end-to-end test of the serializer)
├── examples/
│   ├── bot.py              # foundational single-file example
│   ├── bot.Dockerfile      # image of bot.py with the package from this repository (+ .dockerignore allowlist)
│   └── freeswitch/
│       ├── Dockerfile          # FreeSWITCH 1.11.3 built from source + mod_audio_stream + mod_loopback
│       ├── docker-compose.yml  # FreeSWITCH + echo server; voice bot (profile "bot", extension 9996)
│       └── conf/               # dialplan, SIP profiles, modules.conf.xml, event_socket.conf.xml
├── tests/
│   ├── test_serializer.py
│   ├── test_lookahead.py   # the lookahead processor (run with Pipecat's run_test) and the growing blocks
│   ├── test_control.py     # error paths of FreeSWITCHControl
│   ├── test_esl.py         # error paths of ESLConnection, event parsing
│   ├── test_integration.py # real pipeline + FastAPIWebsocketTransport on uvicorn, client playing mod_audio_stream
│   ├── test_bench.py       # marker "bench": drives bench/run_e2e.py against the running bench (pytest -m bench)
│   └── fake_esl.py         # fake event socket (error replies, drops, auto-completed playback, open-connection count)
├── .github/workflows/      # ci.yml: lint, tests on Python 3.12/3.13 x locked/latest dependencies with branch
│                           #   coverage (fail under 95%), package contents; release.yml: PyPI on a v* tag
├── CHANGELOG.md
├── LICENSE                 # BSD-2
├── README.md
└── pyproject.toml          # uv
```

## No chunker: the transport already chunks

The original design had a `chunker.py` grouping output audio into blocks. It was dropped while implementing v0.1:

- The serializer cannot flush a partial block on its own: it only sends when the transport calls `serialize()`, and
  after the TTS stops no further frame reaches it (`FastAPIWebsocketTransport` inserts no silence between
  utterances, and `TTSStoppedFrame` is not passed to the serializer).
- The output transport already delivers audio in chunks of `audio_out_10ms_chunks` × 10 ms and, on
  `TTSStoppedFrame`, flushes the remainder padded with silence (`base_output.py`, `handle_tts_stopped`).

So each `OutputAudioRawFrame` becomes one `streamAudio` message and the block size is the transport's
`audio_out_10ms_chunks`.

**Revised on 2026-10-28: the block size has a hidden cost.** Every `playback` that starts sends an audio sync
(`switch_channel_audio_sync` in `switch_ivr_play_file`), and on a SIP channel `SWITCH_MESSAGE_INDICATE_AUDIO_SYNC`
resets the jitter buffer and flushes the caller's incoming audio (`switch_rtp_reset_jb` +
`rtp_flush_read_buffer(..., SWITCH_RTP_FLUSH_ONCE)` in `switch_core_media.c`; skipped only for proxied media and T.38).
Each block therefore deafens the channel for 30–40 ms, for every media bug on it (`mod_audio_stream`,
`record_session`). Measured on the bench with a caller leg sending a continuous tone, no bot involved:

| Bot audio played as | Caller audio lost while it plays |
|---|---|
| one 2 s file | 0% |
| 40 ms blocks via `sendmsg` | ~100% (after ~0.5 s; the read side never refills) |
| 200 ms blocks | 16% (14% with the real bot) |
| 1 s blocks | 4% |

With 40 ms blocks the bot never heard the caller while it spoke, so barge-in by voice never worked, on a softphone or
a desk phone (it only worked when a late transcription arrived). The example bot now uses 200 ms blocks (`BLOCK_MS`).
The proper fix is growing blocks: a small first block for latency, then larger ones. Implemented on 2026-11-12, see
[Growing blocks](#growing-blocks-the-lookahead-processor-2026-11-12).

**Corrected on 2026-11-11: the transport paces output at real time, not twice real time.** In
`FastAPIWebsocketOutputTransport`, `_send_interval = (audio_chunk_size / sample_rate) / 2`, and `audio_chunk_size`
is in bytes of 16-bit mono audio, so the interval is exactly the chunk's duration. Only the first two chunks of a
run go out back to back, since the sleep comes after the write: FreeSWITCH holds one chunk of lead and no more.
Measured in process: 40 ms chunks arrive every 40 ms. Earlier versions of this document, the README and a comment
in the serializer said "twice real time" from reading the formula without the units. The consequences:

- `BotStoppedSpeakingFrame` is on time (it was never early).
- The underruns measured with real-time pacing (0–1 per call, ~20 ms) apply to fixed blocks. Waiting for playback
  before the hang-up is still needed: the last block is always still playing.
- With fixed blocks, a block cannot be longer than the first-audio delay one accepts. Blocks can only grow if the
  audio reaches the serializer ahead of the transport's clock.

## Growing blocks: the lookahead processor (2026-11-12)

`serializer.lookahead()` returns a `FreeSWITCHLookahead` frame processor, placed right before `transport.output()`:

```
... -> TTS -> FreeSWITCHLookahead -> transport.output()
                    │ (bot audio, as fast as the TTS produces it)       │ serialize(chunk): one per chunk
                    └──────────────> serializer buffer <────────────────┘ duration, the clock
```

- The processor passes every frame on unchanged and hands each downstream `OutputAudioRawFrame` (default
  destination only) to the serializer, which resamples it, applies the gain and keeps it in a buffer. The transport
  still chunks and paces the same frames, so bot-speaking events and everything built on them stay in real time.
- Once the processor has seen `StartFrame`, the serializer no longer sends the audio of the frames the transport
  writes: each `serialize(audio frame)` is a clock tick. The serializer keeps one number, `ahead`: bytes sent beyond
  what the transport has written. A tick subtracts the chunk; if the buffer is not empty and
  `ahead < max(lookahead_margin_secs, one chunk)`, everything in the buffer (up to 5 s) goes out as one `streamAudio`
  message and is added to `ahead`.
- Why it grows: the first tick sends whatever was produced so far (at least one chunk). The next block leaves only
  when that one is about to run out, and holds what the TTS produced meanwhile: about `k` times the previous block for
  a TTS `k` times faster than real time.
- Why nothing is left behind: with a margin of at least one chunk, the serializer has always sent at least as much
  as the transport has written. The transport's last chunk of an utterance covers all the audio it received (it
  holds back less than a chunk), so that tick sends the rest of the buffer. The floor of one chunk is what makes this
  true when the transport holds back a remainder.
- Audio the transport writes that the processor never saw (padding of the last chunk, end silence, mixer output,
  `OutputDTMFFrame` tones) is not sent and must not count as time: when the buffer is empty, `ahead` is clamped to
  zero.
- `ahead` errs towards sending early. It is reset on every interruption and when `BotStoppedSpeakingFrame` comes
  back upstream through the processor, which is when the transport drops audio it held (the count would otherwise
  stay too high and delay the next blocks).
- Interruption: the processor sees `InterruptionFrame` before the transport writes it. It clears the buffer at once
  and counts the interruption as pending; until the transport's `serialize(InterruptionFrame)` arrives, ticks are
  chunks from before the interruption and send nothing, while audio produced after it stays in the buffer. An
  interruption that never passed the processor clears the buffer when the transport writes it.

Rejected alternatives:

- **Joining files in FreeSWITCH with `playback file_string://a!b!c`.** One file handle, so one audio sync, but
  `mod_dptools` plays 250 ms of silence before the first file of every `file_string` that is not in the channel's
  native format (`context->samples = (handle->samplerate / 1000) * 250` in `next_file`). Measured on the bench: every
  joined playback started with a 250 ms hole. A first implementation batched files in `FreeSWITCHControl` this way and
  was dropped.
- **A faster transport clock** (subclassing the output transport): the pipeline would believe the bot finished
  speaking long before the caller heard it, breaking interruption handling, and it depends on Pipecat's private API.
- **A wall-clock estimate of FreeSWITCH's playout position** in the serializer: the transport's chunks already are
  that clock, and using them keeps the logic deterministic and testable.

Bench results (SIP, PCMU, 3 s tone, the caller sending a continuous tone; "lost" is the share of the caller's tone
that did not reach the bot's pipeline while the bot's tone played):

| Blocks | Source speed | Playbacks | Caller audio lost |
|---|---:|---:|---:|
| 40 ms fixed | any | 75 | ~100% |
| 200 ms fixed | any | 15 | 19–20% |
| lookahead, 40 ms chunks | 1.2× / 3× / 10× | 35 / 8 / 5 | 47% / 10% / 6% |
| lookahead, 200 ms chunks | 1.2× / 3× / 10× | 14 / 5 / 4 | 19% / 7% / 5% |

No gaps in the bot's tone in any run, interruption 15–21 ms, Opus 48 kHz with 5 s blocks (640 kB messages) played
correctly, and the example bot with a real TTS went from 16 playbacks to 3–4 for a 3 s greeting.

## FreeSWITCHFrameSerializer (as implemented)

```python
class FreeSWITCHFrameSerializer(FrameSerializer):
    """Serializer for FreeSWITCH mod_audio_stream (open source edition)."""

    # args: call_uuid (optional, canonical UUID), esl_host, esl_port, esl_password, params, control (optional)
    # params: freeswitch_sample_rate (8000), freeswitch_in_sample_rate (-> freeswitch_sample_rate),
    #   freeswitch_out_sample_rate (-> freeswitch_sample_rate; 8000/16000/24000/32000 raw, 48000 WAV),
    #   output_resampler_quality ("QQ"), output_gain_db (0.0), sample_rate, auto_hang_up (True),
    #   hang_up_timeout_secs (30), lookahead_margin_secs (0.1)
    #   Rates are validated in the constructor.
    #
    # lookahead(): the FreeSWITCHLookahead processor of this serializer (see "Growing blocks")

    # setup(): connect the control if call_uuid is known (setup runs twice: input and output transport)
    # deserialize: FreeSWITCH -> Pipecat
    #   first text message -> channel UUID (plain or {"uuid": ...}); only a canonical UUID -> control.start(uuid)
    #   binary L16         -> InputAudioRawFrame (resampled to the pipeline rate)
    #
    # serialize: Pipecat -> FreeSWITCH
    #   OutputAudioRawFrame  -> {"type":"streamAudio","data":{"audioDataType":"raw"|"wav","sampleRate":N,"audioData":b64}}
    #                           at freeswitch_out_sample_rate (resampled with SOXR "QQ" if the pipeline rate differs)
    #                           and scaled by output_gain_db (clipped to 16 bits)
    #                           (held and merged into the next block until the control is ready)
    #                           with the lookahead processor running: a clock tick, which sends the
    #                           audio taken from the processor when it is less than the margin ahead
    #   InterruptionFrame    -> control.interrupt(sent_messages): uuid_break all, drop stale files
    #   EndFrame             -> control.wait_for_playback() then uuid_kill (if auto_hang_up)
    #   CancelFrame          -> uuid_kill at once (if auto_hang_up)
    #
    # cleanup(): uuid_kill (if auto_hang_up and not done yet) and close the ESL connection; idempotent.
    #   The application calls it from on_client_disconnected (see "Ending the call").
```

```python
class FreeSWITCHControl:
    """ESL client: plays files announced by mod_audio_stream and sends break/kill."""

    # start(uuid): reject a non-canonical UUID (ValueError); connect, filter Unique-ID <uuid>, subscribe
    #   CHANNEL_EXECUTE_COMPLETE CHANNEL_HANGUP_COMPLETE CUSTOM mod_audio_stream::play (guarded by a lock)
    # on play event -> sendmsg <uuid> execute playback <file> (no lead-frames), unless the file index is
    #   below the message count at the last interruption; the index comes from the file name, or from
    #   the arrival order if the name does not match <uuid>_<n>.tmp.<ext> (one-time warning)
    # on CHANNEL_HANGUP_COMPLETE -> mark the channel gone, wake waiters, close the connection
    # interrupt(sent)            -> api uuid_break <uuid> all; drop files with index < sent
    # wait_for_playback(sent, t) -> until every file up to index sent-1 was announced and finished,
    #                               the channel hung up, or the timeout
    # hang_up()                  -> api uuid_kill <uuid>
    # ESL errors (command refused, connection lost) are logged, never raised to the pipeline; a lost
    #   connection counts as the channel gone, so nothing waits for it.
```

### Ending the call (2026-10-28)

The serializer ends the call when the transport passes it `EndFrame` (wait for playback, then `uuid_kill`) or
`CancelFrame` (`uuid_kill` at once). But once the WebSocket is closed, `FastAPIWebsocketOutputTransport._write_frame`
returns before calling the serializer, so a WebSocket that closes first (caller hang-up, network drop) never delivers
the `CancelFrame`. Found by the in-process integration tests: without a fix, that left the ESL connection open and, if
the call was still up, the call parked. Two safeguards:

- `serializer.cleanup()`, which the application calls from `on_client_disconnected` after `worker.cancel()`
  (`examples/bot.py`, `bench/tone_bot.py`, README). No Pipecat transport calls a serializer's cleanup, so this is the
  application's job until Pipecat does it: reported in
  [pipecat-ai/pipecat#6440](https://github.com/pipecat-ai/pipecat/issues/6440), which proposes that the output
  transport's `cleanup()` call the serializer's.
- `FreeSWITCHControl` closes its own connection on `CHANNEL_HANGUP_COMPLETE`, so the connection never outlives the call
  even if the application forgets `cleanup()`.

Points of attention:
- **Block size:** with `sendmsg` there is no fixed cost per file, so blocks can be small (40 ms tested end to end
  with no gap). What remains is the latency × overhead trade-off (base64, one file and one event per block), and,
  as measured on 2026-10-28, how much caller audio each block's playback flushes (see above). The block size is the
  transport's `audio_out_10ms_chunks`; with the lookahead processor it is only the first block's.
- **File order:** `play` events arrive in `streamAudio` order, and `sendmsg` keeps the channel queue in order.
  `uuid_break all` clears the queue, but `play` events in flight can arrive after the break. Fix: the module names
  files `<uuid>_<n>` with `n` increasing per `streamAudio` (`audio_streamer_glue.cpp:430`); the serializer counts
  the `streamAudio` messages it sent and, on a break, drops `play` events whose `n` is below that count. This relies
  on a module internal (regression test + pinned version); a message that fails to decode does not increment `n`.
  If a file name ever stops matching the scheme, the control numbers files in arrival order (which is the send
  order) and logs a warning once, so hang-up and stale-audio dropping keep working.
- **Temp files stay on disk until the stream ends** (mod_audio_stream `da8b068`, `audio_streamer_glue.cpp`). The
  module writes each `streamAudio` block to `<temp_dir>/<uuid>_<n>.tmp.<ext>` (`$${temp_dir}`, usually `/tmp`),
  records the path in `m_Files` (lines 426–455), and deletes them all in `deleteFiles()` (lines 68–83), called only
  from `stream_session_cleanup` (lines 939, 994): on `uuid_audio_stream ... stop` or when the channel closes. Nothing is
  deleted after it has played. So during a call the files pile up: one per block of bot speech, i.e. 1,500 per
  minute of speech with 40 ms blocks (300 with 200 ms blocks), about 0.96 MB per minute of speech at 8 kHz raw,
  1.9 MB at 16 kHz, and 5.8 MB at 48 kHz WAV, whatever the block size. Small blocks multiply the file count, not the
  bytes. If FreeSWITCH crashes, the files of calls in progress stay behind. The package cannot delete them, since it
  does not own the files and runs on another host. This matters for long calls and many concurrent calls (temp
  directory size and inode count), and it is part of the block-size trade-off.
- **Playback queue running dry with real-time sending (measured over SIP, 2026-10-27).** With 40–200 ms blocks and up
  to 50 ms of random delay: 0–1 underrun per call, ~20 ms each. The queue self-heals (after one late block,
  playback runs that much later and gains margin), so total silence is bounded by the largest delay in the call.
  A one-block prebuffer removes almost all of them (it only fails when the delay exceeds the block size), at the
  cost of one block of latency. Not implemented: the transport's own lead is one chunk (the first two go out
  together), and the lookahead processor sends the speech further ahead (at least `lookahead_margin_secs`).
- **`uuid_kill` on `EndFrame` cuts off the end of the speech (confirmed).** Kill right after the last block: the last
  file never started and 240 ms of speech were lost (200 ms blocks). Waiting for the last file's
  `CHANNEL_EXECUTE_COMPLETE`: nothing lost, hang-up 270 ms after the last block. Implemented in v0.1 (with a
  timeout); end to end, the hang-up comes 25–60 ms after the last audio the caller hears.
- **~40 ms lost at the start of each talkspurt (SIP): on the receiving side, not the sending side.** The bench's
  caller leg (a FreeSWITCH receiving) loses ~40 ms whenever audio resumes after silence. The packet capture
  (`bench/rtp_wire.py`) shows the RTP sent by the bot leg is complete, with no sequence or timestamp gaps; the only
  gap on the wire is the real ~20 ms underrun. It does not affect the package; it would only matter if the bot's
  FreeSWITCH sent to another FreeSWITCH, and then it would be that receiver's behavior.
- **Bench with `mod_loopback`:** loses the last 20 ms frame of each back-to-back file (`playback` sends an audio sync
  when it starts and the loopback clears its outbound queue). It is a loopback artifact; audio measurements must use
  SIP (`--endpoint sip`, the default).
- **`BotStoppedSpeakingFrame` timing:** the transport paces audio at real time, so the frame comes when the caller
  has heard the speech, give or take the playback latency (tens of milliseconds). An earlier note here said it could
  come early; that followed from the wrong "twice real time" reading and was never observed.
- **Hang-up:** `uuid_kill` tears the channel down in ~20 ms and the WebSocket closes in ~15 ms. The module does not
  raise `mod_audio_stream::disconnect` on hang-up, only when the server closes the connection.
- **Deployment:** the bench's ESL listens on `127.0.0.1` only. In production the bot needs access to the event socket
  (ACL + password), documented in the README, and the WebSocket needs the token (see
  [Security of the WebSocket](#security-of-the-websocket-2026-10-28)).
- **Sample rates (measured 2026-10-27, PCMU and Opus):** bot audio must reach FreeSWITCH at the channel rate;
  per-file resampling in FreeSWITCH leaves artefacts at every seam. Opus runs at 48 kHz inside FreeSWITCH, and raw
  audio cannot be played at 48 kHz (`.r48` is not a mod_sndfile format), so 48 kHz goes as WAV. Resampling in the
  serializer must hold nothing back (SOXR "QQ"): nothing flushes the resampler at the end of an utterance. Input and
  output rates are separate parameters (16 kHz caller audio for STT/VAD, 48 kHz bot audio on Opus).
  `freeswitch_out_sample_rate` defaults to `freeswitch_sample_rate` (8000), not to the pipeline rate: a 24 kHz TTS on
  a G.711 channel is resampled by the serializer (0 glitchy windows on the bench) instead of block by block by
  FreeSWITCH (71 of 286). The channel rate is the negotiated one, not the codec's nominal rate: Opus ran at 48 kHz
  with Linphone and at 16 kHz with a Grandstream GXV3275 (`Set Codec ... opus/16000`), and sending 48 kHz to the
  latter clicked at the seams. The dialplan passes `${write_rate}` to the bot (2026-10-28).
- **Bot audio level (2026-10-28):** TTS speech arrived around −13 dBFS RMS with full-scale peaks, about 12 dB above
  telephone level, and distorted on both phones; `output_gain_db` (−12 dB in the example) fixes it.
- **ptime:** measurements used 20 ms. With another ptime the `uuid_broadcast` gap would change (5 × ptime); with
  `sendmsg` it should not matter, but check it on the bench with different codecs.

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
- **v0.1 (implemented on 2026-10-27):** serializer + control (ESL), `bot.py` example, README, CHANGELOG, unit tests,
  end-to-end test on the bench. Revised on 2026-10-28 after the project review (security fixes, rate default,
  `cleanup()`, error-path and in-process integration tests, explicit sdist contents). Listed on 2026-10-28: 0.1.0 on
  PyPI, demo video in the README, Discord post; the Pipecat docs PR (pipecat-ai/docs#1611) was merged on
  2026-10-30.
- **v0.2:** DTMF (implemented on 2026-11-03, see [DTMF](#dtmf-2026-11-03)). The other candidates were left out of
  this release: adapters for `mod_audio_fork` and `mod_earshot`'s native protocol, outbound ESL, and a per-call
  token.
- **v0.3:** growing blocks through the lookahead processor (implemented on 2026-11-12, see
  [Growing blocks](#growing-blocks-the-lookahead-processor-2026-11-12)). Still open: latency metrics (time to first
  audio per block size) and playing `OutputDTMFFrame` tones with lookahead.

## Listing checklist (community integrations guide)

- [x] Code with Pipecat-style docstrings
- [x] Foundational single-file example (`examples/bot.py`; run on 2026-10-28 in a softphone call to the bench with
  STT, LLM and TTS on OpenRouter, barge-in included; the Deepgram/OpenAI/Cartesia/ElevenLabs paths not yet)
- [x] README: intro, installation, usage, how to run the example, tested Pipecat version
- [x] BSD-2 LICENSE
- [x] CHANGELOG
- [x] PyPI release (0.1.0, 2026-10-28)
- [x] Hang-up through the native API (ESL)
- [x] DTMF → `InputDTMFFrame` (v0.2)
- [x] Fork of pipecat-ai/docs: row in `supported-services.mdx`, page `serializers/freeswitch.mdx`, `docs.json`
  (pipecat-ai/docs#1611, merged on 2026-10-30)
- [x] 30–60 s video showing it working + an interruption (README, Demo section)
- [x] Post in the Discord `#community-integrations` channel
