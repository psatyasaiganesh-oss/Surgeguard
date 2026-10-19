FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY burnwatch ./burnwatch
COPY tests ./tests
COPY examples ./examples
USER 10001:10001
RUN python -m unittest discover -v
ENTRYPOINT ["python", "-m", "burnwatch"]
CMD ["demo"]
