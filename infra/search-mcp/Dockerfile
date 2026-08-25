FROM python:3.12-slim

RUN pip install --no-cache-dir "h11==0.16.0" "httpcore==1.0.9" "httpx>=0.28,<1" "httpx2==2.12.0" "mcp==2.1.0" "sse-starlette==3.0.2"
WORKDIR /app
COPY server.py /app/server.py
USER 10001:10001
CMD ["python", "/app/server.py"]
