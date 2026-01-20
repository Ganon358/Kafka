from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "events-json"
BOOTSTRAP_SERVERS = "localhost:9092"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )

    event_types = ["login", "logout", "purchase", "click", "signup"]

    try:
        for i in range(1, 6):
            event = {
                "event_id": i,
                "event_type": event_types[(i - 1) % len(event_types)],
                "timestamp": utc_iso(),
                "source": "tp1-producer",
            }

            meta = producer.send(TOPIC, event).get(timeout=10)
            print(f"Envoyé => {event} | partition={meta.partition} offset={meta.offset}")
            time.sleep(1)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
