# Use an official lightweight Python image
FROM python:3.9-slim

# Set environment variables for best practices in Python containers
ENV PYTHONUNBUFFERED True

# Set the working directory inside the container
WORKDIR /app

# Copy the dependencies file and install them
# This is done in a separate step to leverage Docker's build cache
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of your application files into the container
COPY . .

# Command to run the Gunicorn server
# It will listen on port 8080, which is what Cloud Run expects
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]