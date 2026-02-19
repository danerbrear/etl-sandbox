import time
import json
import jsonlines
from models import Event, Transaction
from pydantic import ValidationError

type LoadedEvents = list[Event]
type TransformedEvents = list[Transaction]

def load() -> tuple[bool, LoadedEvents | None]:
    print("\nLoading events from stream...")

    # Get offset
    offset: int = 0
    try:
        with open('../broker/offset.txt', 'r') as file:
            file_content = file.read()
        offset = int(file_content)
        print(f"Starting offset: {offset}")
    except Exception as err:
        print(f"Could not fetch offset, setting to file start. {str(err)}") 

    # Load Events
    try:
        events = []
        invalid_events: list[tuple[dict, str]] = []
        with jsonlines.open('../broker/events.jsonl') as reader:
            it = reader.iter()
            for _ in range(offset):
                next(it, None) 
            for line in it:
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

def transform(events: LoadedEvents) -> TransformedEvents:
    print("\nTransforming...")
    transformed_events = []
    failed_events: list[Event, str] = []
    for event in events:
        # Check idempotency

        try:
            # Validate and apply business rules to the event
            transaction = Transaction.from_event(event)

            # Enrich

            # Add to list of validated and normalized events
            transformed_events.append(transaction)
        except KeyError as err:
            failed_events.append((event, str(err)))

    print(f"Found {len(transformed_events)} successfully processed events and {len(failed_events)} failed events.")

    return transformed_events

def extract():
    print("\nExtracting...")

def main():
    success, events = load()
    if not success or events is None:
        print("Failed to load events.")
        exit(1)
    print("Successfully loaded events!")

    transform(events)

    extract()

    print("ETL job complete!")

if __name__ == "__main__":
    print("Starting event consumer...")
    try:
        while(True):
            main()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Consumer stopped")