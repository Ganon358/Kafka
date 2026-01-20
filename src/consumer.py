from __future__ import annotations

import json
from kafka import KafkaConsumer

TOPIC = "events-json"
BOOTSTRAP_SERVERS = "localhost:9092"


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="tp1-consumer-group",
        auto_offset_reset="latest",  # ne relit pas l'historique
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("Consumer démarré (JSON). En attente de messages... CTRL+C pour arrêter.")

    try:
        for msg in consumer:
            print(f"Reçu <= {msg.value}")
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
