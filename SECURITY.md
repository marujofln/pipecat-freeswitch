# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately, through GitHub's
[private vulnerability reporting](https://github.com/marujofln/pipecat-freeswitch/security/advisories/new)
("Report a vulnerability" on the repository's Security tab). Do not open a public issue.

Include what you found, how to reproduce it, and the version of `pipecat-freeswitch`, Pipecat and FreeSWITCH. You
should get a first answer within a week. Fixes are released as a new version and noted in the CHANGELOG's
"Security" section.

## Supported versions

Only the latest release receives security fixes while the project is in the 0.x series.

## Deployment notes

`pipecat-freeswitch` controls calls through the FreeSWITCH event socket, which grants full control of FreeSWITCH.
The README's [FreeSWITCH setup](README.md#freeswitch-setup) section covers the measures this relies on:

- **Event socket:** reachable only from the bot's host, restricted with an ACL (`apply-inbound-acl`), with a dedicated
  password instead of FreeSWITCH's default `ClueCon` (the package logs a warning when the default is used).
- **WebSocket:** require a token on the handshake (`STREAM_EXTRA_HEADERS` in the dialplan, checked before
  `accept()`, as `examples/bot.py` does), and use `wss://` or a private network, since over `ws://` the token travels
  in clear text.
- **Channel UUID:** the serializer accepts only canonical UUIDs, so the WebSocket's first message cannot inject event
  socket commands.

The test bench in `examples/freeswitch/` uses fixed, public credentials and is meant for local testing only.
