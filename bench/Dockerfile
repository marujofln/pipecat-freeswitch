FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY esl.py echo_server.py ./
CMD ["python", "echo_server.py"]
