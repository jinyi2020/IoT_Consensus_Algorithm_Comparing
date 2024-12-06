# Use an official Python runtime as the base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy only the requirements file first (for caching)
COPY requirements.txt .

COPY Nodes_map.json .

# Copy only necessary files (Python scripts and additional required files)
COPY *.py ./

# Install any Python dependencies
#RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -r requirements.txt


# Expose the port for communication
EXPOSE 5000-5100

# Command to run your application
#CMD ["python", "raft_v0.py"]
ENTRYPOINT ["python", "raft_v2.py"]