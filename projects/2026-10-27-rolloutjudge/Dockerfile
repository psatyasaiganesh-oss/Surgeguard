FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --create-home --uid 10001 app
WORKDIR /app
COPY --chown=app:app . .
USER app

CMD ["python", "-m", "rolloutjudge", "demo"]
