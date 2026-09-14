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
COPY voting_api.py .
COPY stats_commands_api.py .
COPY leaderboard_api.py .
COPY admin_request_api.py .
COPY connection_keeper.py .

EXPOSE 8080

CMD ["sh", "-c", "uvicorn connection_keeper:app --host 0.0.0.0 --port ${PORT:-8080}"]
