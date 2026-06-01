FROM rust:latest AS builder
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config build-essential clang llvm-dev libclang-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/app/target \
    LIBCLANG_PATH="$(llvm-config --libdir)" cargo build --release --features whisper-rs-backend
COPY src ./src
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/app/target \
    touch src/main.rs \
    && LIBCLANG_PATH="$(llvm-config --libdir)" cargo build --release --features whisper-rs-backend \
    && cp /app/target/release/media_subtitle_worker /app/media_subtitle_worker

FROM python:3.11-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg \
    && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/pyannote-venv \
    && /opt/pyannote-venv/bin/pip install --upgrade pip
COPY pyannote/requirements.txt ./pyannote/requirements.txt
ARG PYTORCH_VERSION=2.6.0
ARG PYTORCH_INDEX_URL=https://download.pytorch.org/whl/cpu
RUN /opt/pyannote-venv/bin/pip install --no-cache-dir \
        "torch==${PYTORCH_VERSION}" "torchaudio==${PYTORCH_VERSION}" \
        --index-url "${PYTORCH_INDEX_URL}" \
    && /opt/pyannote-venv/bin/pip install --no-cache-dir -r /app/pyannote/requirements.txt
COPY pyannote ./pyannote
COPY --from=builder /app/media_subtitle_worker /usr/local/bin/app
ENV TRANSCRIBER_BACKEND=whisper-rs
ENV PYANNOTE_ENABLED=true
ENV PYANNOTE_PYTHON_BIN=/opt/pyannote-venv/bin/python
ENV PYANNOTE_SCRIPT_PATH=/app/pyannote/diarize.py
CMD ["app"]