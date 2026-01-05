import json
from kafka import KafkaConsumer


def create_consumer():
    """Create a new Kafka consumer instance"""
    return KafkaConsumer(
        "pharmacy_sales",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="pharmacy_sales_group",
    )


def read_batch(limit=1000, consumer=None, max_batches=None):
    """
    Read messages from Kafka in batches

    Args:
        limit: Maximum messages per batch
        consumer: Optional existing consumer instance. If None, creates new one.
        max_batches: Maximum number of batches to yield before stopping

    Yields:
        List of messages (batch)
    """
    # Create consumer if not provided
    should_close = False
    if consumer is None:
        consumer = create_consumer()
        should_close = True

    try:
        message = []
        batch_count = 0

        # use poll for not just waiting for limit but yielding remaining messages as well
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)

            for tp, messages in msg_pack.items():
                for msg in messages:
                    message.append(msg.value)
                    if len(message) >= limit:
                        yield message
                        message = []
                        batch_count += 1

                        # Stop if max_batches reached
                        if max_batches and batch_count >= max_batches:
                            if message:
                                yield message
                            return

            # if one second passed and no messages were received, yield remaining messages
            if not msg_pack and message:
                yield message
                message = []
                batch_count += 1

                # Stop if max_batches reached
                if max_batches and batch_count >= max_batches:
                    return

            # If no messages for a while and max_batches set, stop
            if not msg_pack and not message and max_batches:
                return

    finally:
        # Only close if we created the consumer
        if should_close:
            consumer.close()


# Usage example
if __name__ == "__main__":
    print("Starting to consume messages...")
    try:
        # Using read_batch with max_batches to limit consumption
        for batch in read_batch(limit=500, max_batches=10):
            print(f"Received batch of {len(batch)} messages")
            # Process first message as example
            if batch:
                print(f"First message: {batch[0]}")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    print("Done!")
