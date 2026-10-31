FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
RUN useradd --create-home --uid 10001 slicequeue \
    && mkdir /data \
    && chown slicequeue:slicequeue /data

COPY --chown=slicequeue:slicequeue pyproject.toml README.md ./
COPY --chown=slicequeue:slicequeue slicequeue ./slicequeue

USER slicequeue
ENTRYPOINT ["python", "-m", "slicequeue"]
CMD ["demo"]
