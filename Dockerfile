FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY rotation_api.py .
COPY message_everyone_api.py .
COPY stats_tracker_api.py .
COPY access_management_api.py .
COPY admin_logs_api.py .

EXPOSE 8080

CMD ["sh", "-c", "uvicorn admin_logs_api:app --host 0.0.0.0 --port ${PORT:-8080}"]
