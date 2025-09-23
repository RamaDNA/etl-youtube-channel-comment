FROM apache/airflow:3.0.6

# Copy requirements.txt dari root project ke container
COPY ../requirements.txt /tmp/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt