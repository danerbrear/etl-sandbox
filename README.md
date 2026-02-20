# Overview

This project is designed to be a learning sandbox environment to practice building data-processing systems in Python. Our goal is to take in stream events delivered at least once and display metrics about the data.

## Data Source

The system will receive events that can be delivered out of order and more than once, to emulate an event-driven architecture from a message broker or queue.

Events will be structured like the following:

```
{
  "event_id": "evt_abc123",
  "event_type": "transaction_created",
  "user_id": 42,
  "amount": 19.99,
  "currency": "USD",
  "timestamp": "2025-01-01T12:01:05Z",
  "source_service": "payments-service"
}
```

## ETL Service

We will use a single Python AWS Lambda to handle Extraction, Trasformation, and Loading. The Lambda should be split up into each step of the ETL job to handle the following responsibilities

- Extract
    - Receive the event starting from most recent offset
- Transform
    - Validate required fields and convert strings to enums
    - De-duplicate
    - Enrich with metadata like processed_time
- Load
    - Upload to the data-store (local CSV file in this project)

## Metrics

- Average payment value
- Percentage of events per service
