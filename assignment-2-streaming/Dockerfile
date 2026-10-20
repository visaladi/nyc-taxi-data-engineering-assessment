FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY streaming ./streaming

RUN useradd --create-home --uid 10001 app
USER app

CMD ["python", "-m", "streaming.processor"]
