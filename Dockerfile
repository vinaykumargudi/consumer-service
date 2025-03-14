# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables for AWS and Kafka credentials
# ENV AWS_ACCESS_KEY_ID=AKIAQXUIX6ZL45CWOUEG
# ENV AWS_SECRET_ACCESS_KEY=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
# ENV AWS_DEFAULT_REGION=us-east-1
# ENV KAFKA_USERNAME=doadmin
# ENV KAFKA_PASSWORD=AVNS_6REZIAvZBNULzEJOQOK
# ENV KAFKA_BROKER=db-kafka-nyc3-64952-do-user-9992548-0.l.db.ondigitalocean.com:25073
# ENV KAFKA_TOPIC=stock_transactions
# ENV CA_CERT_PATH=/app/crt.pem

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r req.txt

# Expose the port the app runs on (default for FastAPI with Uvicorn is 8000)
EXPOSE 8001

# Run the application with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
