FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/opt/huggingface

WORKDIR /app
COPY pyproject.toml ./

# Keep the expensive market/embedding dependency layer stable when only app
# source files change. Hatchling only needs a minimal package to resolve and
# install the dependency groups at this stage.
RUN mkdir -p backend/app && touch backend/app/__init__.py
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && pip install ".[markets,embeddings]"
RUN mkdir -p "$HF_HOME" && python -c "from transformers import AutoTokenizer; AutoTokenizer.from_pretrained('Qwen/Qwen2.5-7B-Instruct', revision='a09a35458c702b33eeacc393d103063234e8bc28')"

COPY backend ./backend
RUN --mount=type=cache,target=/root/.cache/pip pip install --no-deps .

RUN useradd --create-home --uid 10001 agent && chown -R agent:agent /app "$HF_HOME"
USER agent

CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
