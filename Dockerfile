# Python image
FROM python:3.8

# working directory in the container
WORKDIR /app

# Copy the files to the app directory
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# entrypoint to run travelbooking etl 
CMD ["python", "main.py"]
