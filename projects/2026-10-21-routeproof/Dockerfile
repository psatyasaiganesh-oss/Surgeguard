FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN addgroup --system routeproof && adduser --system --ingroup routeproof routeproof
COPY --chown=routeproof:routeproof . /app
USER routeproof
CMD ["python", "-m", "routeproof", "demo"]
