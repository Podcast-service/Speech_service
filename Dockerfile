FROM rust:latest AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config build-essential clang llvm-dev libclang-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN LIBCLANG_PATH="$(llvm-config --libdir)" cargo build --release --features whisper-rs-backend

COPY src ./src
RUN touch src/main.rs \
    && LIBCLANG_PATH="$(llvm-config --libdir)" cargo build --release --features whisper-rs-backend

FROM python:3.11-slim
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/pyannote-venv \
    && /opt/pyannote-venv/bin/pip install --upgrade pip

COPY pyannote/requirements.txt ./pyannote/requirements.txt
RUN /opt/pyannote-venv/bin/pip install --no-cache-dir -r /app/pyannote/requirements.txt

COPY pyannote ./pyannote

COPY --from=builder /app/target/release/media_subtitle_worker /usr/local/bin/app

ENV TRANSCRIBER_BACKEND=whisper-rs
ENV PYANNOTE_ENABLED=true
ENV PYANNOTE_PYTHON_BIN=/opt/pyannote-venv/bin/python
ENV PYANNOTE_SCRIPT_PATH=/app/pyannote/diarize.py

CMD ["app"]
