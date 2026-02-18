import time
import json
from jsonlines import open
from models import Event
from pydantic import ValidationError

type LoadedEvents = list[Event]

def load() -> tuple[bool, LoadedEvents | None]:
    print("\nLoading events from stream...")
    try:
        events = []
        invalid_events: list[tuple[dict, str]] = []
        with open('../broker/events.jsonl') as reader:
            for line in reader:
                try:
                    event = Event(
                        event_id=line.get("event_id", None),
                        event_type=line.get("event_type", None),
                        user_id=line.get("user_id", None),
                        amount=line.get("amount", None),
                        currency=line.get("currency", None),
                        timestamp=line.get("timestamp", None),
                        source_service=line.get("source_service", None))
                    events.append(event)
                except ValidationError as e:
                    invalid_events.append((line, str(e)))

            print(f"Found {len(events)} valid events and {len(invalid_events)} invalid events.")

            if len(invalid_events) > 0:
                print("First invalid event:")
                event, message = invalid_events[0]
                print(json.dumps(event, indent=4))
                print(message)

        return (True, events)
    except FileNotFoundError:
        print("File not found")
        return (False, None)

def transform():
    print("Transforming...")

def extract():
    print("Extracting...")

def main():
    success, events = load()
    if not success or events is None:
        print("Failed to load events.")
        exit(1)
    print("Successfully loaded events!")

    transform()
    extract()

if __name__ == "__main__":
    print("Starting event consumer...")
    try:
        while(True):
            main()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Consumer stopped")