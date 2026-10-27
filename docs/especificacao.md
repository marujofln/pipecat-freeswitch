# Especificação — pipecat-freeswitch (rascunho v2)

Desenho técnico definido na sessão de 2026-10-27 e revisado no mesmo dia com os resultados da Fase 0
(bancada em `examples/freeswitch/`, números em `examples/freeswitch/README.md`).

## Objetivo

Permitir que um bot Pipecat atenda/origine chamadas num FreeSWITCH usando o `mod_audio_stream` open source,
com barge-in e hang-up funcionando, empacotado como integração comunitária listada nas docs do Pipecat.

## Arquitetura

```
Chamador ──SIP/RTP──> FreeSWITCH ──(mod_audio_stream, WebSocket)──> FastAPIWebsocketTransport
                         ▲   │                                          │ FreeSWITCHFrameSerializer
                         │   └─ evento mod_audio_stream::play ─┐        ▼
                         │                                     ▼
                         └──── ESL: sendmsg execute playback <── FreeSWITCHControl (ESL inbound)
                               uuid_break all / uuid_kill
                                                                pipeline: VAD → STT → LLM → TTS
```

- **Entrada** (FreeSWITCH → bot): binário L16 mono, 8k ou 16k, pacotes de `STREAM_BUFFER_SIZE` ms (padrão 20).
  Medido: 640 bytes por pacote a 16 kHz, intervalo médio 19,9 ms, p95 41,8 ms (às vezes chegam em pares).
- **Saída** (bot → FreeSWITCH): JSON `streamAudio` com áudio base64 em blocos. O módulo **só grava** o bloco em
  `$temp/<uuid>_<n>.tmp.r16` e dispara o evento `mod_audio_stream::play` (JSON com campo `file`).
  **Ele não toca nada.** Quem toca é o `FreeSWITCHControl`, que recebe o evento por ESL e enfileira
  `playback <file>` no canal.
- **Reprodução: usar ESL `sendmsg`, não `uuid_broadcast`.** O `uuid_broadcast` põe `lead-frames: 5` fixo em cada
  comando (`src/switch_ivr_async.c:5737` no FreeSWITCH 1.11.3; 5736 na 1.10.12), e o canal descarta 5 quadros antes de cada
  arquivo (`src/switch_ivr.c:540-554`): 100 ms de silêncio por arquivo a 20 ms de ptime. Com `sendmsg`
  (`call-command: execute`, `execute-app-name: playback`, `event-lock: true`) a reprodução é contínua e o primeiro
  áudio sai 24–41 ms depois do `streamAudio` (1.11.3).
- **Controle:** `uuid_break <uuid> all` para barge-in (sem `all` só para o arquivo atual e o resto da fila continua
  tocando) e `uuid_kill <uuid>` para hang-up, pelo mesmo ESL.

### Decisão: canal de controle = ESL inbound para tudo (2026-10-27)

| Função | Precisa de | ESL | `mod_xml_rpc` |
|---|---|:-:|:-:|
| Receber `mod_audio_stream::play` | assinatura de eventos | sim | não (só pedido/resposta) |
| Tocar sem folga | `sendmsg execute` (não há comando `api` equivalente sem `lead-frames`) | sim | não |
| Saber quando o último arquivo terminou | `CHANNEL_EXECUTE_COMPLETE` | sim | não |
| `uuid_break all`, `uuid_kill` | comando `api` | sim | sim |

O ESL é obrigatório para a reprodução, e o `mod_xml_rpc` só cobriria break/kill, com mais um módulo, porta e
credencial. A recomendação do guia do Pipecat ("hang-up pela API nativa, idealmente `aiohttp`") mira provedores
REST; no FreeSWITCH a API nativa de controle é o ESL.

- Uma conexão ESL por chamada: aberta no `setup()` do serializer, `filter Unique-ID <uuid>` assim que o UUID chega,
  fechada no fim da chamada.
- Cliente próprio em asyncio puro (base: `bench/esl.py`), sem dependência nova.
- Segurança: ESL equivale a controle total do FreeSWITCH (como o `/webapi` do `mod_xml_rpc`); exigir rede privada,
  ACL e senha própria, e documentar.
- v0.2: ESL outbound (`socket bot:8084 async full` no dialplan) como alternativa de deploy, sem expor a 8021.

## Estrutura do repositório

```
pipecat-freeswitch/
├── src/pipecat_freeswitch/
│   ├── __init__.py
│   ├── serializer.py       # FreeSWITCHFrameSerializer
│   ├── control.py          # FreeSWITCHControl: ESL (play via sendmsg, uuid_break all, uuid_kill)
│   └── chunker.py          # agrupa áudio de saída em blocos para streamAudio
├── bench/                  # bancada da Fase 0: echo_server.py, esl.py, run_experiments.py
├── examples/
│   ├── bot.py              # exemplo fundamental, arquivo único
│   └── freeswitch/
│       ├── Dockerfile          # FreeSWITCH 1.11.3 compilado + mod_audio_stream + mod_loopback
│       ├── docker-compose.yml  # FreeSWITCH + servidor da bancada (depois: + bot)
│       └── conf/               # dialplan, sip profile, modules.conf.xml, event_socket.conf.xml
├── tests/
│   ├── test_serializer.py
│   └── fixtures/           # amostras L16 8k/16k
├── .github/workflows/      # lint, testes, publicação no PyPI
├── CHANGELOG.md
├── LICENSE                 # BSD-2
├── README.md               # em inglês
└── pyproject.toml          # uv
```

## FreeSWITCHFrameSerializer (esboço)

```python
class FreeSWITCHFrameSerializer(FrameSerializer):
    """Serializer for FreeSWITCH mod_audio_stream (open source edition)."""

    # params: sample_rate (8000|16000), chunk_ms, auto_hang_up=True,
    #         control: FreeSWITCHControl, call_uuid opcional (senão vem da 1ª msg de texto)

    # deserialize: FreeSWITCH -> Pipecat
    #   1ª mensagem texto  -> metadados (UUID do canal) -> control.attach(uuid) começa a tocar os arquivos
    #   binário L16        -> InputAudioRawFrame (resample se taxa != pipeline)
    #   JSON de evento     -> log; DTMF -> InputDTMFFrame (v0.2)
    #
    # serialize: Pipecat -> FreeSWITCH
    #   OutputAudioRawFrame  -> chunker -> {"type":"streamAudio","data":{"audioDataType":"raw","sampleRate":N,"audioData":b64}}
    #   InterruptionFrame    -> descarta buffer do chunker + control.uuid_break(uuid, all=True)
    #   EndFrame/CancelFrame -> flush/descarte + control.uuid_kill(uuid) se auto_hang_up
```

```python
class FreeSWITCHControl:
    """ESL client: plays files announced by mod_audio_stream and sends break/kill."""

    # connect(host, port, password); filter Unique-ID <uuid>;
    # subscribe CUSTOM mod_audio_stream::play CHANNEL_EXECUTE_COMPLETE
    # on play event for an attached uuid -> sendmsg <uuid> execute playback <file> (sem lead-frames)
    # interrupt(uuid) -> api uuid_break <uuid> all
    # hang_up(uuid)   -> espera o CHANNEL_EXECUTE_COMPLETE do último arquivo, depois api uuid_kill <uuid>
```

Pontos de atenção:
- **Tamanho de bloco:** com `sendmsg` não há custo fixo por arquivo, então o bloco pode ser pequeno (100 ms medido
  sem folga). O compromisso que sobra é latência × overhead (base64, um arquivo e um evento por bloco), que é a
  pergunta experimental em aberto. Revalidar com blocos de 20–60 ms.
- **Pacing (PR pipecat-ai/pipecat#5593, corrigido no Pipecat 1.10.0):** se o serializer não emite payload para um
  frame de áudio, o transport não faz o pacing e o `BotStoppedSpeakingFrame`/`EndFrame` chegam cedo. Como o chunker
  acumula frames (retornando `None` em vários), verificar esse comportamento e fixar Pipecat ≥ 1.10.0.
- **Flush** do último bloco parcial no fim da fala do bot (ex.: em `TTSStoppedFrame`).
- **Ordem dos arquivos:** os eventos `play` chegam na ordem dos `streamAudio`; o `sendmsg` mantém a fila do canal
  em ordem. Interromper com `uuid_break all` limpa a fila, mas eventos `play` em trânsito podem chegar depois do
  break. Solução: o módulo nomeia os arquivos `<uuid>_<n>` com `n` crescente por `streamAudio`
  (`audio_streamer_glue.cpp:430`); o serializer conta os `streamAudio` enviados e, no break, descarta eventos `play`
  com `n` menor que a contagem. Depende de detalhe interno do módulo (teste de regressão + versão fixada); uma
  mensagem com erro de decodificação não incrementa `n`.
- **Fila esvaziando com envio no ritmo real (não medido).** A bancada mandou o áudio em rajada. Com o pacing do
  transport, o bloco *i+1* fica pronto `chunk_ms` depois do *i* e começa a tocar no instante em que o *i* termina:
  margem zero, qualquer atraso vira silêncio, e piora com blocos pequenos. Mitigação provável: segurar o primeiro
  bloco até ter folga (ex.: 1 bloco extra). Medir antes de escrever o chunker.
- **`uuid_kill` no `EndFrame` corta o fim da fala.** Quando o Pipecat entrega o último frame, o FreeSWITCH ainda tem
  o último bloco (e a fila) para tocar; kill imediato corta as últimas palavras. O controle espera o
  `CHANNEL_EXECUTE_COMPLETE` do último arquivo antes do `uuid_kill`.
- **`BotStoppedSpeakingFrame` cedo:** chega até um bloco antes do fim real do áudio. Efeito pequeno na detecção de
  turno; documentar.
- **Hang-up:** `uuid_kill` derruba o canal em ~20 ms e a WebSocket fecha em ~17 ms. O módulo não dispara
  `mod_audio_stream::disconnect` no hang-up, só quando o servidor fecha a conexão.
- **Deploy:** o ESL da bancada escuta só em `127.0.0.1`. Em produção o bot precisa de acesso ao event socket
  (ACL + senha); documentar no README.
- **ptime:** a medição foi com 20 ms. Com outro ptime a folga do `uuid_broadcast` mudaria (5 × ptime); com `sendmsg`
  não deve importar, mas vale conferir no harness com codecs diferentes.

## Dialplan de teste (validado na bancada)

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

O esboço anterior não passava o `<uuid>` como primeiro argumento de `uuid_audio_stream`.

## Roadmap

- **Fase 0 — bancada (concluída em 2026-10-27):** docker-compose com FreeSWITCH 1.11.3 compilado +
  `mod_audio_stream` (amigniter, commit `da8b068`) + `mod_event_socket` + `mod_loopback`; servidor de eco FastAPI;
  `bench/run_experiments.py` com chamadas loopback. Respondido: chegada do L16, latência por bloco, origem da folga
  de 100 ms (`lead-frames` do `uuid_broadcast`), `uuid_break` com e sem `all`, `uuid_kill`.
- **v0.1:** serializer + chunker + control (ESL), exemplo `bot.py`, docker-compose, README, CHANGELOG, testes de
  serializer, vídeo 30–60 s com interrupção, PR nas docs do Pipecat. Suficiente para listagem.
- **v0.2:** DTMF (ESL ou bind no dialplan), adaptadores para `mod_audio_fork` e protocolo nativo do `mod_earshot`.
- **v0.3:** métricas de latência (time-to-first-audio por tamanho de bloco).

## Checklist de listagem (guia de integrações comunitárias)

- [ ] Código com docstrings no padrão Pipecat
- [ ] Exemplo fundamental em arquivo único
- [ ] README: intro, instalação, uso, como rodar exemplo, versão do Pipecat testada
- [x] LICENSE BSD-2
- [ ] CHANGELOG, publicação no PyPI
- [ ] Hang-up via API nativa (ESL); DTMF → `InputDTMFFrame` (se suportado)
- [ ] Fork de pipecat-ai/docs: linha em `supported-services.mdx`, página `serializers/freeswitch.mdx`, `docs.json`
- [ ] Vídeo 30–60 s mostrando funcionamento + interrupção
- [ ] Post no `#community-integrations` do Discord
