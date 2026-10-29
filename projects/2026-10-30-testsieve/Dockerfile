FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY --chown=65532:65532 . /app

USER 65532:65532

CMD ["python", "-m", "testsieve", "demo"]
