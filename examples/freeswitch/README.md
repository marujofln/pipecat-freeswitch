# Phase 0 bench

FreeSWITCH 1.11.3 (built from source, minimal modules) + `mod_audio_stream` (amigniter, community edition) +
a FastAPI echo server (`../../bench/echo_server.py`).

```bash
cd examples/freeswitch
docker compose build        # first build compiles FreeSWITCH: ~15-30 min
docker compose up
```

Register a softphone as `1000` / `1234` against the host IP, port 5060, and dial `9999`, or run the
automated experiments (no softphone needed, standard library only):

```bash
python3 bench/run_experiments.py                       # input, latency, break, kill
python3 bench/run_experiments.py -e latency --chunks 100,500
```

The runner originates `loopback/9999` calls. The caller leg plays a 1 kHz tone and records what it hears;
the dialplan leg streams to the echo server. Results go to `captures/results-<timestamp>.json`.

## What the bench answers

Everything is logged per call to `captures/<uuid>.jsonl` (wall-clock timestamps) and the raw caller audio to
`captures/last_input.l16` (`ffplay -f s16le -ar 16000 -ac 1 captures/last_input.l16`).

| Question | How |
|---|---|
| L16 arrival, packet size | `audio_in` events; change `STREAM_BUFFER_SIZE` in `docker-compose.yml` / dialplan |
| `streamAudio` latency vs block size | `CHUNK_MS` env var; compare `stream_audio_sent` → `play_event` → `play_queued` |
| File queueing | `MODE=tone` pushes 10 s of tone in one burst; listen for overlap or serialization |
| `uuid_break` | `curl localhost:8765/break/<uuid>` and `curl 'localhost:8765/break/<uuid>?all=true'` |
| `uuid_kill` | `curl localhost:8765/kill/<uuid>` |

## Finding that changes the design

The open source module does **not** play `streamAudio` itself. It writes `$temp/<uuid>_<n>.tmp.r16` and fires
the custom event `mod_audio_stream::play` (JSON with a `file` field). Something has to play the file. The bench does
it with an ESL listener that queues `playback <file>` on the channel. The package's `FreeSWITCHControl` does the
same, so ESL is required. Barge-in and hang-up go over the same ESL connection (`mod_xml_rpc` is not used).

Also: the `uuid_audio_stream` call in the spec's dialplan sketch was missing the `<uuid>` first argument.

## Results (2026-10-27, FreeSWITCH 1.11.3, mod_audio_stream `da8b068`, loopback calls, 16 kHz)

The same experiments were first run on 1.10.12; the numbers below are from 1.11.3 and match within a few ms.
Raw results: `captures/results-*.json` (not committed).

**Input.** 640-byte frames (20 ms of L16 at 16 kHz), mean inter-arrival 19.9 ms, p95 41.5 ms
(frames sometimes arrive in pairs).

**Output latency vs block size** (3 s tone sent in one burst). The echo server can play each file two ways
(`--play-method`):

- `broadcast`: `uuid_broadcast <uuid> <file> aleg`
- `sendmsg`: ESL `sendmsg` with `call-command: execute`, `execute-app-name: playback` (default)

| Block (ms) | Method | Files played / sent | First audio (ms) | Playback span for 3 s (s) | Gap between files (ms, mean) |
|---:|---|---:|---:|---:|---:|
| 100  | broadcast | 25 / 30* | 125 | 4.94 | 102 |
| 100  | sendmsg   | 30 / 30  |  41 | 3.00 |   0 |
| 200  | broadcast | 15 / 15  | 127 | 4.43 | 100 |
| 200  | sendmsg   | 15 / 15  |  41 | 3.01 |   2 |
| 300  | broadcast | 10 / 10  | 146 | 3.90 | 101 |
| 300  | sendmsg   | 10 / 10  |  38 | 3.01 |   0 |
| 500  | broadcast |  6 / 6   | 123 | 3.52 | 105 |
| 500  | sendmsg   |  6 / 6   |  24 | 3.01 |   0 |
| 1000 | broadcast |  3 / 3   | 124 | 3.21 | 101 |
| 1000 | sendmsg   |  3 / 3   |  35 | 3.01 |   0 |

\* the call was hung up before the queue drained.

With `sendmsg` the caller's recording shows one unbroken tone for every block size.

### Where the 100 ms gap comes from

`uuid_broadcast` (`switch_ivr_broadcast`, `src/switch_ivr_async.c:5737` in FreeSWITCH 1.11.3, line 5736 in 1.10.12) queues each
`playback` as an `execute` command with a hardcoded `lead-frames: 5` header. When the channel processes the command
(`switch_ivr_parse_event`, `src/switch_ivr.c:540-554`), it first reads and discards 5 non-CNG frames before
running the app. At 20 ms per frame that is 100 ms of silence before **every** file, and before the first one too.
Occasional 120 ms gaps are a sixth frame read while a CNG frame was skipped. With a 30 ms ptime the gap would be
150 ms.

Sending the same `execute` via ESL `sendmsg` without `lead-frames` removes the gap: playback becomes continuous
and first audio drops to 24-41 ms.
**Playback in the package must use `sendmsg` (or another path without `lead-frames`), not `uuid_broadcast`.**

**Barge-in** (8 s tone as 500 ms files, break 2 s in):

| Command | Files started after break | Audio heard after break |
|---|---:|---:|
| `uuid_break <uuid>` | 7-9 | 3.2-4.0 s (queue keeps playing) |
| `uuid_break <uuid> all` | 0 | 22-24 ms |

Same behavior with `broadcast` and `sendmsg`. `uuid_break` without `all` only stops the current file.
The serializer must use `all`.

**Hang-up.** `uuid_kill` over ESL: channel hung up in 21 ms (`NORMAL_CLEARING`), WebSocket closed in 15 ms. The module does not raise `mod_audio_stream::disconnect` on hang-up, only when the server
drops the connection.
