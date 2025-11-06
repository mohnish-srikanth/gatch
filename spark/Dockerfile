FROM bitnami/spark:latest
WORKDIR /app
COPY spark_job.py .
# CMD ["spark-submit", "./spark_job.py"]
CMD ["tail", "-f", "/dev/null"]
