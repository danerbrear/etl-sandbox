"""
Local event producer that publishes to a broker (no consumer in this project).
  Producer → broker log file (JSONL). Another project can consume from the broker.
Event shape matches README.md (event_id, event_type, user_id, amount, currency, timestamp, source_service).
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


def project_root() -> Path:
    """Repository root (parent of producer/)."""
    return Path(__file__).resolve().parent.parent


def broker_log_path() -> Path:
    """Path to the broker log file (JSONL) at project root. Creates broker dir if missing."""
    path = project_root() / "broker" / "events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def new_event() -> dict:
    """Build one event with the structure from README.md."""
    return {
        "event_id": random.randint(1, 100),
        "event_type": "transaction_created",
        "user_id": hash(uuid.uuid4().hex) % 100_000,
        "amount": round(1.0 + (hash(uuid.uuid4().hex) % 10000) / 100.0, 2),
        "currency": "USD",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_service": "payments-service",
    }


def publish(broker_path: Path, event: dict) -> None:
    """Append one event as a JSON line to the broker log (mimics publish to broker)."""
    with open(broker_path, "a") as f:
        f.write(json.dumps(event) + "\n")


def main() -> None:
    path = broker_log_path()
    print(f"Publishing events every 1s to broker: {path}")
    print("Stop with Ctrl+C.")
    try:
        while True:
            event = new_event()
            publish(path, event)
            print(f"  {event['timestamp']}  {event['event_id']}  user={event['user_id']}  amount={event['amount']} {event['currency']}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
