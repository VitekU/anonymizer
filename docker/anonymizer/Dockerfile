FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake git pkg-config ca-certificates \
        librdkafka-dev libcapnp-dev capnproto \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cd src && capnp compile -oc++ http_log.capnp

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build --parallel

CMD ["./build/consumer"]