"""Print how many messages per second a topic is receiving, from its end offsets."""
import argparse
import time

from confluent_kafka import Consumer, TopicPartition


def end_offsets(consumer: Consumer, topic: str, partitions: list[int]) -> int:
    """Sum the log-end offsets. Only their growth over time matters."""
    return sum(consumer.get_watermark_offsets(TopicPartition(topic, p))[1] for p in partitions)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("topic")
    parser.add_argument("--seconds", type=float, default=30)
    args = parser.parse_args()

    # Only reads offsets. It subscribes to nothing, and the group is never used.
    consumer = Consumer({"bootstrap.servers": "localhost:9094", "group.id": "topic-rate"})
    partitions = list(consumer.list_topics(args.topic, timeout=10).topics[args.topic].partitions)

    start = end_offsets(consumer, args.topic, partitions)
    time.sleep(args.seconds)
    grown = end_offsets(consumer, args.topic, partitions) - start
    consumer.close()

    print(f"{args.topic}: {grown} messages in {args.seconds:.0f} s = {grown / args.seconds:.0f}/sec")


if __name__ == "__main__":
    main()
