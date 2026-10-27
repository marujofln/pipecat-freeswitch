# FreeSWITCH (built from source, minimal module set) + mod_audio_stream (amigniter, community edition).
# Built from source because the SignalWire apt repo requires a personal access token.
FROM debian:12-slim AS build

ARG FS_VERSION=v1.11.3
# FreeSWITCH 1.11.3 requires sofia-sip >= 1.13.18 and spandsp >= 3.1.1.
ARG SOFIA_VERSION=v1.13.19
ARG SPANDSP_VERSION=v3.1.1
# mod_audio_stream main as of 2026-10-27.
ARG MAS_REF=da8b068eb16708d33252366adbcd8b231f82f419

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake pkg-config git ca-certificates autoconf automake libtool libtool-bin \
        yasm nasm uuid-dev zlib1g-dev libjpeg-dev libsqlite3-dev libcurl4-openssl-dev libpcre2-dev \
        libspeex-dev libspeexdsp-dev libedit-dev libssl-dev libldns-dev libtiff-dev libsndfile1-dev \
        libevent-dev libopus-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone --depth 1 --branch ${SOFIA_VERSION} https://github.com/freeswitch/sofia-sip.git \
    && cd sofia-sip && ./bootstrap.sh && ./configure && make -j"$(nproc)" && make install \
    && cd .. && git clone --depth 1 --branch ${SPANDSP_VERSION} https://github.com/freeswitch/spandsp.git \
    && cd spandsp && ./bootstrap.sh && ./configure && make -j"$(nproc)" && make install && ldconfig

RUN git clone --depth 1 --branch ${FS_VERSION} https://github.com/signalwire/freeswitch.git
WORKDIR /src/freeswitch
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
# Minimal module set: SIP, loopback (automated tests), XML dialplan, ESL (control channel), file formats.
RUN ./bootstrap.sh -j \
    && cat > modules.conf <<'MODS'
loggers/mod_console
loggers/mod_logfile
endpoints/mod_sofia
endpoints/mod_loopback
dialplans/mod_dialplan_xml
applications/mod_commands
applications/mod_dptools
applications/mod_expr
codecs/mod_opus
formats/mod_native_file
formats/mod_sndfile
formats/mod_tone_stream
event_handlers/mod_event_socket
MODS
RUN ./configure --prefix=/usr/local/freeswitch \
    && make -j"$(nproc)" && make install && make samples

RUN git clone https://github.com/amigniter/mod_audio_stream.git /src/mas \
    && cd /src/mas && git checkout ${MAS_REF} && git submodule update --init --recursive \
    && mkdir build && cd build \
    && PKG_CONFIG_PATH=/usr/local/freeswitch/lib/pkgconfig:/usr/local/lib/pkgconfig \
       cmake -DCMAKE_BUILD_TYPE=Release .. && make -j"$(nproc)" \
    && install -m 0755 mod_audio_stream.so /usr/local/freeswitch/lib/freeswitch/mod/

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        libuuid1 zlib1g libjpeg62-turbo libsqlite3-0 libcurl4 libpcre2-8-0 libspeex1 libspeexdsp1 libedit2 \
        libssl3 libldns3 libtiff6 libsndfile1 libevent-2.1-7 libevent-pthreads-2.1-7 libopus0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/freeswitch /usr/local/freeswitch
COPY --from=build /usr/local/lib /usr/local/lib
RUN ldconfig
# Replace the sample config with the bench config (mounted over it by docker-compose).
ENV PATH=/usr/local/freeswitch/bin:$PATH
EXPOSE 5060/udp 5060/tcp 8021/tcp 16384-16484/udp
CMD ["freeswitch", "-nf", "-nonat", "-c"]
