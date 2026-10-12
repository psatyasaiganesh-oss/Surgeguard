FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY ledgergate ./ledgergate

USER 65534:65534
ENTRYPOINT ["python", "-m", "ledgergate"]
CMD ["demo", "--payments", "400", "--seed", "13"]
