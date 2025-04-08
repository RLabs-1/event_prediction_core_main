# Use the official Python image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the main.py script into the container
COPY main.py .

# Install the required Python packages
RUN pip install kafka-python python-dotenv

# Define the command to run the script
CMD ["python", "main.py"]
