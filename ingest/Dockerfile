# Use the official Python image as the base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file into the container at /app
COPY requirements.txt /app/

# Install any dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire content of the current directory into the container at /app
COPY . /app/

# Run your Python script
CMD ["python3", "mqtt_subscribe.py"]
