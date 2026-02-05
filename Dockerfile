FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application files
COPY main.py supabase_conf.py ./

# Default command (can be overridden in Cloud Run Job)
CMD ["python3", "main.py", "--startAuctionCrawlCount", "1", "--endAuctionCrawlCount", "10"]

