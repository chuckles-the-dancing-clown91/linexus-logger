# Linexus Logger — operational audit-log service.
FROM rust:1-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config build-essential ca-certificates git && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release --bin linexus-logger

FROM debian:stable-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 linexus \
    && mkdir -p /var/lib/linexus && chown linexus /var/lib/linexus
COPY --from=build /app/target/release/linexus-logger /usr/local/bin/linexus-logger
USER linexus
ENV LOGGER_BIND=0.0.0.0:5151 \
    LOGGER_DATABASE_URL=sqlite:///var/lib/linexus/logger.sqlite
EXPOSE 5151
CMD ["linexus-logger"]
