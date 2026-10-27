# Pesquisa — sessão de 2026-10-27

Notas da sessão de chat que originou o projeto. Números de estrelas e versões são do momento da pesquisa.

## 2. Pipecat

- Repo: https://github.com/pipecat-ai/pipecat — BSD-2, ~16.8k estrelas, mantido pela Daily.
- v1.0.0 em 2026-04-14; v1.3.0 (2026-05-29) trouxe framework multiagente.
- Modelo: pipeline de processadores trocando frames (áudio, texto, controle). STT → LLM → TTS com VAD (Silero),
  Smart Turn, interrupções e streaming ponta a ponta.
- Ecossistema: Pipecat Flows (máquina de estados), Whisker (debugger), Tail (dashboard terminal), CLI `pipecat init`,
  eval harness `pipecat eval run`, Pipecat Cloud.
- Crítica conhecida: deploy em escala fica com o usuário; gravidade comercial puxa para Daily WebRTC / Pipecat Cloud.

## 3. Panorama FreeSWITCH ↔ Pipecat

- Catálogo de serviços: https://docs.pipecat.ai/api-reference/server/services/supported-services
  Serializers: Acefone, Asterisk (comunitário), AwaazAI, Bandwidth, didlogic, Exotel, Genesys, Plivo, Telnyx,
  Twilio, Vonage, Wavix. **Sem FreeSWITCH.**
- `pipecat-asterisk` (modelo a seguir): https://github.com/NikolayShakin/pipecat-asterisk
  - `AsteriskFrameSerializer` + `AsteriskWebsocketTransport` com controle de fluxo (low/high water marks).
  - Trabalha com `slin`; recomenda igualar sample rate da pipeline ao do canal para evitar resample duplo.
  - Exemplo com docker-compose do Asterisk e extensões por sample rate; script de teste de qualidade por taxa.
  - Testado com Pipecat v1.1.0, Python 3.12+.
- Prior art no lado FreeSWITCH:
  - `mod_earshot` (https://github.com/wiringai/mod_earshot): módulo full-duplex com vários protocolos, incluindo
    Pipecat via protobuf; métricas de latência e eventos ESL tipados. Afirma que a reprodução do `mod_audio_stream` não é confiável.
  - `aplisay/llm-agent` tem um `FreeSwitchAudioStreamSerializer` interno (não empacotado):
    https://github.com/aplisay/llm-agent/pull/325
- Forks do `mod_audio_stream`: amigniter (original), rasonyang, saravanansaru, connexcs. A v1.0.3 com stream binário
  bidirecional é comercial (grátis até 10 canais, sem código-fonte).

## 4. Protocolo do mod_audio_stream (open source)

- `uuid_audio_stream <uuid> start <ws-url> <mono|mixed|stereo> <8k|16k> [metadata]` — envia L16; metadata é enviada antes do áudio.
- `uuid_audio_stream <uuid> send_text|stop|pause|resume`.
- Canal var `STREAM_BUFFER_SIZE` (ms, múltiplo de 20) define tamanho do pacote enviado.
- Eventos: `mod_audio_stream::json|connect|disconnect|error|play`.
- Playback: servidor envia
  `{"type":"streamAudio","data":{"audioDataType":"raw|wav|mp3|ogg","sampleRate":8000,"audioData":"<base64>"}}`;
  o módulo salva em arquivo temporário e toca (evento `mod_audio_stream::play` com o caminho).
- Fonte: https://github.com/connexcs/mod_audio_stream (README idêntico ao do amigniter).

## 5. Requisitos de listagem como integração comunitária

Fonte: https://github.com/pipecat-ai/pipecat/blob/main/COMMUNITY_INTEGRATIONS.md
- Repo com código, exemplo fundamental em arquivo único, README (intro, instalação, uso na pipeline, como rodar o exemplo,
  versão do Pipecat testada), LICENSE permissiva (BSD-2), docstrings, CHANGELOG.
- Nome `pipecat-{vendor}`; classe `VendorFrameSerializer`.
- Telefonia: hang-up pela API nativa (idealmente `aiohttp`); DTMF → `InputDTMFFrame` com `KeypadEntry`, inválido → `None`.
- Serializer não é FrameProcessor: ações em `EndFrame`/`CancelFrame` só pelo caminho de frames.
- Erros: `push_error` + exceção. Testes: exemplo como teste de integração, `pipecat eval run`, unit tests opcionais.
- Docs: fork de https://github.com/pipecat-ai/docs, linha no `supported-services.mdx`, página em
  `api-reference/server/services/serializers/freeswitch.mdx` com badge `CommunityMaintained`, entrada no `docs.json`.
- PR com vídeo de 30–60 s mostrando funcionamento e uma interrupção; avisar no canal `#community-integrations` do Discord.

