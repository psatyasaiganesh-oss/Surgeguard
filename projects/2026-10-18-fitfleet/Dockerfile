FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN groupadd --gid 10001 fitfleet && useradd --uid 10001 --gid fitfleet --no-create-home fitfleet
WORKDIR /app
COPY fitfleet ./fitfleet
COPY tests ./tests
RUN python -m unittest discover -v
USER 10001:10001
ENTRYPOINT ["python", "-m", "fitfleet"]
CMD ["demo"]
