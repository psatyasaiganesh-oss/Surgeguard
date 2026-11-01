FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
RUN useradd --create-home --uid 10001 exitpack \
    && mkdir /data \
    && chown exitpack:exitpack /data

COPY --chown=exitpack:exitpack pyproject.toml README.md ./
COPY --chown=exitpack:exitpack exitpack ./exitpack

USER exitpack
ENTRYPOINT ["python", "-m", "exitpack"]
CMD ["demo"]
