FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HF_HOME=/opt/huggingface
RUN pip install --no-cache-dir "fastembed>=0.7,<1" "transformers==4.55.4"
RUN mkdir -p "$HF_HOME" && python -c "from transformers import AutoTokenizer; AutoTokenizer.from_pretrained('Qwen/Qwen2.5-7B-Instruct', revision='a09a35458c702b33eeacc393d103063234e8bc28')"
RUN useradd --create-home --uid 10001 adapter && chown -R adapter:adapter "$HF_HOME"
WORKDIR /app
COPY server.py /app/server.py
USER adapter
EXPOSE 8092
CMD ["python", "/app/server.py"]
