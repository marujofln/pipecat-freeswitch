# The example voice bot (examples/bot.py) with pipecat-freeswitch installed from this repository.
# Build context: the repository root (see examples/freeswitch/docker-compose.yml, service "bot").
# bot.Dockerfile.dockerignore keeps the context to the files below.
FROM python:3.12-slim

WORKDIR /app

# Pipecat pinned to the version the package is tested with; both TTS providers, so TTS_PROVIDER works.
RUN pip install --no-cache-dir \
    "pipecat-ai[websocket,deepgram,openai,cartesia,elevenlabs,silero]==1.14.0" \
    python-dotenv uvicorn

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .

COPY examples/bot.py ./

CMD ["python", "bot.py"]
