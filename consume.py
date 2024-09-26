import logging

from threading import Event, Thread

from src.consumer import Consumer
from src import utils


def start_consuming():
    cli_args = utils.cli_parse_args()
    logging.info(f"Starting app with args={vars(cli_args)}")
    redis_client = utils.establish_redis_connection(
        cli_args.redis_host, cli_args.redis_port
    )
    stop_event = Event()
    # Reset the messages:processed:count to 0
    redis_client.set("messages:processed:count", 0)
    consumers = []
    for _ in range(cli_args.consumer_group_size):
        consumer = Consumer(redis_client, "messages:published", utils.message_handler)
        consumers.append(consumer)
    try:
        for consumer in consumers:
            consumer.start()

        reporting_thread = Thread(
            target=utils.report_messages_per_second, args=(redis_client, stop_event)
        )
        reporting_thread.start()

        for consumer in consumers:
            consumer.join()
    except KeyboardInterrupt:
        logging.info(
            "[SIGNAL] Received signal=KeyboardInterrupt, shutting down consumers..."
        )
    finally:
        stop_event.set()  # Signal the reporting thread to stop
        for consumer in consumers:  # Stop all consumers
            consumer.stop()
        reporting_thread.join()  # Wait for the reporting thread to finish
        logging.info("All consumers and reporting thread have been terminated.")
        logging.info("App is shutting down...")


if __name__ == "__main__":
    start_consuming()
