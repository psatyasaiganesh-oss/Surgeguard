FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY quorumforge ./quorumforge

USER 65534:65534
ENTRYPOINT ["python", "-m", "quorumforge"]
CMD ["demo", "--keys", "600", "--seed", "14"]
