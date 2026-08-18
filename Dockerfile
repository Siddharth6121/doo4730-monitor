# Container for the always-on DOO4730 failure monitor (instant_monitor.py)
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# runs the continuous real-time monitor; secrets come from environment variables
CMD ["python", "instant_monitor.py"]
