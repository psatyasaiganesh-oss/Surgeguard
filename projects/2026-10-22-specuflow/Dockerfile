FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN addgroup --system specuflow && adduser --system --ingroup specuflow specuflow
COPY --chown=specuflow:specuflow . /app
USER specuflow
CMD ["python", "-m", "specuflow", "demo"]
