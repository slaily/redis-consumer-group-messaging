import random
import argparse
import logging
import time

from threading import Event

import redis

from redis.retry import Retry
from redis.exceptions import (
    TimeoutError,
    ConnectionError,
    ResponseError,
)
from redis.backoff import ExponentialBackoff


def cli_parse_args(args: list[str] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Consumer group application for processing messages from Redis."
    )
    parser.add_argument(
        "--consumer-group-size",
        type=int,
        default=3,
        help="Number of consumers in the group (default: 3)",
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default="localhost",
        help="Redis server host (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis server port (default: 6379)",
    )

    if args is None:
        return parser.parse_args()

    return parser.parse_args(args)


def establish_redis_connection(redis_host: str, redis_port: int) -> redis.Redis:
    """Establish a connection to Redis."""
    redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        retry=Retry(
            ExponentialBackoff(cap=30, base=1),
            10,  # Retry 10 times with exponential backoff
        ),
        retry_on_error=[
            ConnectionError,
            TimeoutError,
            ConnectionResetError,
            ConnectionRefusedError,
            ResponseError,
        ],
        health_check_interval=60,
        socket_timeout=10,
        socket_connect_timeout=10,
        decode_responses=True,
    )
    try:
        redis_client.ping()
        logging.info("[Redis] Successfully connected.")

        return redis_client
    except ConnectionError as conn_err:
        logging.error(
            f"[Redis] Failed to connect exception_type={type(conn_err)} exception_message={conn_err}"
        )
        raise


def message_handler(message: dict) -> dict:
    """Simulate message processing by adding a random property and value"""
    random_value = random.random()
    message["metadata"] = {"random_property": random_value}

    return message


def report_messages_per_second(redis_client: redis.Redis, stop_event: Event) -> None:
    """
    Periodically report (3 second intervals) the number of messages processed per second.
    """
    last_count = 0
    last_time = time.time()

    while not stop_event.is_set():
        time.sleep(3)
        current_time = time.time()
        current_count = int(redis_client.get("messages:processed:count") or 0)
        messages_processed = current_count - last_count
        time_elapsed = current_time - last_time
        messages_per_second = messages_processed / time_elapsed
        logging.info(
            f"[Monitoring] Messages processed per second count={int(messages_per_second)}"
        )
        last_count = current_count
        last_time = current_time

    return None
