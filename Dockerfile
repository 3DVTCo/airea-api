FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY airea_api_server_v2.py .
COPY conversation_persistence.py .
COPY airea_brain/ ./airea_brain/

EXPOSE 8000

CMD ["uvicorn", "airea_api_server_v2:app", "--host", "0.0.0.0", "--port", "8000"]
