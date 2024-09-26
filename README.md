# Redis Consumer Group Application

This project uses Python to create a consumer group that processes messages from Redis Pub/Sub. It shows how multiple consumers can efficiently handle messages from a Redis channel at the same time.

## Features

- Multiple consumers running concurrently
- Redis connection with retry and error handling
- Command-line argument parsing for easy configuration
- Message processing simulation
- Active consumer ID management

## Requirements

- Python 3.9+
- Redis server
- Required Python packages

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/redis-consumer-group.git
   cd redis-consumer-group
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Run unit tests:
   ```
   make unit-test
   ```

## Usage

1. Start the Redis server using Docker:
   ```
   make run-redis
   ```

2. Start the consumer group application (Terminal 1):
   ```
   python consume.py --consumer-group-size 3 --redis-host localhost --redis-port 6379
   ```
   You can adjust the number of consumers and Redis connection details as needed.

3. Run the publisher script to push events (Terminal 2):
   ```
   python scripts/publisher.py
   ```

4. (Optional) Monitor the application progress using Redis Stack with Insights:
   ```
   make run-redis-stack
   ```
   Then open a web browser and navigate to `http://localhost:8001` to access the Redis Insights dashboard.

Note: Make sure to stop and remove the Docker containers when you're done.
