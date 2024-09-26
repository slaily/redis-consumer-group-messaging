import logging

from uuid import uuid4
from time import sleep
from typing import Callable
from json import loads, dumps
from threading import Thread, Event

import redis


# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)


class Consumer(Thread):
    """
    A consumer thread that processes messages from a Redis channel.

    It subscribes to a specified Redis channel, continuously listens for incoming messages,
    and processes them using a provided message handler function. The consumer
    implements message deduplication and distributed locking to ensure each
    message is processed exactly once in a multi-consumer environment.

    Attributes:
        _id (UUID): A unique identifier for this consumer instance.
        _redis_client (redis.Redis): The Redis client used for communication.
        _pubsub (redis.client.PubSub): The PubSub object for channel subscription.
        _message_handler (Callable): The function used to process messages.
        _stop_event (threading.Event): An event to signal the thread to stop.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        channel: str,
        message_handler: Callable,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._id = uuid4()
        self._redis_client = redis_client
        self._pubsub = self._redis_client.pubsub()
        self._pubsub.subscribe(channel)
        self._message_handler = message_handler
        self._stop_event = Event()

    @property
    def id(self):
        return self._id

    def _acquire_lock(self, message_id: str) -> bool:
        """Try to acquire a lock for processing a message."""
        lock_key = f"lock:{message_id}"
        return self._redis_client.set(lock_key, str(self.id), nx=True, ex=30)

    def _release_lock(self, message_id: str) -> None:
        """Release the lock for a processed message."""
        lock_key = f"lock:{message_id}"
        self._redis_client.delete(lock_key)

        return None

    def _is_message_processed(self, message_id: str) -> bool:
        """Check if the message has already been processed."""
        return self._redis_client.sismember("messages:processed:ids", message_id)

    def _mark_message_as_processed(self, message_id: str) -> None:
        """Mark the message as processed."""
        self._redis_client.sadd("messages:processed:ids", message_id)

    def _consume(self) -> None:
        """Consume messages"""
        while not self._stop_event.is_set():
            message = self._pubsub.get_message()
            if message and message["type"] == "message":
                try:
                    message_data = loads(message["data"])
                    message_id = message_data.get("message_id")

                    if not message_id:
                        continue

                    # Check if the message has already been processed
                    if self._is_message_processed(message_id):
                        continue

                    # Try to acquire the lock for this message
                    if self._acquire_lock(message_id):
                        try:
                            # Double-check if the message has been processed after acquiring the lock
                            if self._is_message_processed(message_id):
                                continue
                            # Process the message
                            processed_message = self._message_handler(message_data)
                            self._redis_client.incr("messages:processed:count")
                            # Store processed message data in Redis Stream
                            stream_entry = {
                                "message_id": message_id,
                                "data": dumps(processed_message),
                            }
                            self._redis_client.xadd("messages:processed", stream_entry)
                            # Mark the message as processed
                            self._mark_message_as_processed(message_id)
                        finally:
                            # Release the lock
                            self._release_lock(message_id)
                except Exception as exc:
                    logging.error(
                        f"Error processing message id={message_id} exception={exc}"
                    )
            sleep(0.01)  # Small delay to prevent CPU overuse

        return None

    def run(self) -> None:
        """Run the consumer thread.

        This method performs the following tasks:
        1. Adds the consumer's ID to the active consumer list in Redis.
        2. Starts the message consumption process.
        3. Continues running until the stop event is set.

        The consumer thread will process messages from the subscribed Redis channel,
        handle them according to the defined message handler, and update the
        necessary counters and data structures in Redis.
        """
        self._redis_client.rpush("consumer:ids", str(self.id))
        self._consume()

        return None

    def stop(self) -> None:
        """Stop the consumer thread.

        This method performs the following tasks:
        1. Sets the stop event to signal the consumer thread to stop.
        2. Waits for the thread to finish its current operations.
        3. Removes the consumer's ID from the active consumer list in Redis.

        The method ensures a graceful shutdown of the consumer thread,
        allowing it to complete any ongoing message processing before terminating.
        """
        if not self._stop_event.is_set():
            self._stop_event.set()
            self.join()
            self._redis_client.lrem("consumer:ids", 1, str(self.id))

        return None
