FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application files
COPY main.py supabase_conf.py ./

# Default command (can be overridden in Cloud Run Job)
# No default arguments - will process all URLs unless specified
CMD ["python3", "main.py"]

