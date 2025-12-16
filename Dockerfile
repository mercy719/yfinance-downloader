FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy script
COPY download_data.py .

# Run the script
CMD ["python", "download_data.py"]
