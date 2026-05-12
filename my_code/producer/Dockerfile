FROM python:3.11-slim
WORKDIR /app

# Snappy support comes from python-snappy 0.7+ which is a pure-Python shim around
# cramjam (Rust, ships manylinux wheels). No system libsnappy needed.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
CMD ["python", "-u", "producer.py"]
