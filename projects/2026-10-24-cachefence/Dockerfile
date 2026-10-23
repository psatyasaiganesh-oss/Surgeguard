FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY --chown=10001:10001 . /app

USER 10001:10001

CMD ["python", "-m", "cachefence", "demo"]
