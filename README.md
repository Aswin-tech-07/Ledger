# Ledger Service (Golang)

Implements a minimal banking ledger with:
- **API** service (create accounts, enqueue transactions, query ledger)
- **Worker** service (processes transactions from RabbitMQ)
- **PostgreSQL** for account balances with ACID-like guarantees (row locks, idempotency)
- **MongoDB** for append-only ledger
- **RabbitMQ** for asynchronous transaction processing
- **Docker Compose** to run everything locally

## Run

```bash
docker compose up --build
```

Seed schema:
```bash
docker compose exec -T postgres psql -U ledger -d ledger < ./db/migrations/001_init.sql
```

### Endpoints

- `POST /accounts` -> `{ "owner":"Alice", "initial_balance": 10000 }`
- `POST /transactions` -> `{ "account_id": "...", "type":"deposit|withdraw", "amount":500, "idempotency_key":"<uuid>" }`
- `GET /accounts/{id}/ledger` -> returns ledger records

## Tests

Add your DB URL as env and run `go test ./...`. Unit tests include idempotency and handler validation.
