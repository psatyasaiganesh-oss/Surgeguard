FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN groupadd --gid 10001 shiftsafe && useradd --uid 10001 --gid shiftsafe --no-create-home shiftsafe
WORKDIR /app
COPY shiftsafe ./shiftsafe
COPY tests ./tests
RUN python -m unittest discover -v
USER 10001:10001
ENTRYPOINT ["python", "-m", "shiftsafe"]
CMD ["demo"]
