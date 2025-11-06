FROM python:3.10-slim
WORKDIR /app
RUN pip install openai pandas pyarrow
COPY insight_worker.py .
# CMD ["python", "insight_worker.py"]
CMD ["tail", "-f", "/dev/null"]
