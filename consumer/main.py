import time
import json
import jsonlines
from models import Event, Transaction
from pydantic import ValidationError

type LoadedEvents = list[Event]
type TransformedEvents = list[Transaction]

def extract() -> tuple[bool, LoadedEvents | None]:
    print("\Extracting events from stream...")

    # Get offset
    offset: int = 0
    try:
        with open('../database/offset.txt', 'r') as file:
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

    # Get idempotency keys
    keys = set()
    try:
        with open('../database/idempotency.txt', 'r') as file:
            file_content = file.read()
            keys = set(file_content.split(','))
            print(keys)
    except:
        print('Failed to open idempotency table')

    transformed_events = []
    failed_events: list[tuple[Event, str]] = []
    events_skipped = 0
    for event in events:
        try:
            # Check idempotency
            if event.event_id in keys:
                events_skipped += 1
                continue

            # Validate and apply business rules to the event
            transaction = Transaction.from_event(event)

            # Add to list of validated and normalized events
            transformed_events.append(transaction)
        except Exception as err:
            failed_events.append((event, str(err)))

    print(f"Successfully processed count: {len(transformed_events)}")
    print(f"Skipped events count: {events_skipped}")
    print(f"Failure count: {len(failed_events)}")

    if len(failed_events) > 0:
        event, message = failed_events[0]
        print(f"\nFirst failed event: {message}\n{event}")

    return transformed_events

def load(transformed_events: TransformedEvents) -> bool:
    print("\nLoading...")
    return True

def print_metrics():
    pass

def commit_progress() -> bool:
    return True

def main():
    success, events = extract()
    if not success or events is None:
        print("Failed to load events.")
        exit(1)
    print("Successfully loaded events!")

    transformed_events = transform(events)
    if transformed_events is None:
        print("Failed to transform events")
        exit(1)

    success = load(transformed_events)
    if not success:
        print("Failed to load data")
        exit(1)
    
    success = commit_progress()
    if not success:
        print("Failed to commit job progress")
        exit(1)

    print("ETL job complete!")
    print_metrics()

if __name__ == "__main__":
    print("Starting event consumer...")
    try:
        while(True):
            main()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Consumer stopped")