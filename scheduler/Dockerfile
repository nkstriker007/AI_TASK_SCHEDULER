# atsd, the scheduler service. Build from the repo root:
#   docker build -f scheduler/Dockerfile -t ats-atsd .
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake ca-certificates libhiredis-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY scheduler/ scheduler/
RUN cmake -S scheduler -B /build -DCMAKE_BUILD_TYPE=Release -DATS_BUILD_TESTS=OFF \
    && cmake --build /build -j --target atsd ats_sim ats_validate

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libhiredis0.14 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/atsd /build/ats_sim /build/ats_validate /usr/local/bin/
WORKDIR /data
# SIGTERM ends the main loop cleanly (in-flight attempts are not tracked across restarts yet).
STOPSIGNAL SIGTERM
CMD ["atsd"]
