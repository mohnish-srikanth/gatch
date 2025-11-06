FROM python:3.10-slim
WORKDIR /app
RUN pip install streamlit pandas pyarrow
COPY app.py .
# CMD ["streamlit", "run", "app.py", "--server.port=8501"]
CMD ["tail", "-f", "/dev/null"]
