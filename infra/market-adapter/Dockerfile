FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir "akshare>=1.16,<2"
RUN useradd --create-home --uid 10001 adapter
WORKDIR /app
COPY server.py /app/server.py
USER adapter
EXPOSE 8091
CMD ["python", "/app/server.py"]
