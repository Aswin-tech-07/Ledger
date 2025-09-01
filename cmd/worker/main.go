package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Message struct {
	MessageID   string    `json:"message_id"`
	AccountID   string    `json:"account_id"`
	Type        string    `json:"type"` // "deposit" or "withdraw"
	Amount      int64     `json:"amount"`
	RequestedAt time.Time `json:"requestedAt"`
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	dbURL := getenv("DATABASE_URL", "postgres://ledger:password@localhost:5432/ledger")
	amqpURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	queue := getenv("QUEUE_NAME", "transactions")
	mongoURL := getenv("MONGO_URL", "mongodb://localhost:27017")
	mongoDB := getenv("MONGO_DB", "ledger")

	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	mc, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Fatalf("mongo: %v", err)
	}
	defer mc.Disconnect(ctx)
	coll := mc.Database(mongoDB).Collection("ledger")

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("amqp: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("amqp channel: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("queue: %v", err)
	}

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}

	log.Println("worker consuming...")
	for d := range msgs {
		var m Message
		if err := json.Unmarshal(d.Body, &m); err != nil {
			log.Printf("bad message: %v", err)
			d.Nack(false, false)
			continue
		}
		if err := processMessage(ctx, db, coll, &m); err != nil {
			log.Printf("process error (will retry): %v", err)
			d.Nack(false, true) // requeue for retry
			continue
		}
		d.Ack(false)
	}
}

func processMessage(ctx context.Context, db *pgxpool.Pool, ledger *mongo.Collection, m *Message) error {
	// idempotency
	if _, err := uuid.Parse(m.MessageID); err != nil {
		return errors.New("invalid message id")
	}
	// transactional balance update with row-level lock
	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Check idempotency
	var already string
	err = tx.QueryRow(ctx, `SELECT message_id FROM applied_messages WHERE message_id=$1`, m.MessageID).Scan(&already)
	if err == nil {
		// already applied
		return nil
	}

	// Lock the account row
	var balance int64
	err = tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id=$1 FOR UPDATE`, m.AccountID).Scan(&balance)
	if err != nil {
		return err
	}

	switch m.Type {
	case "deposit":
		balance += m.Amount
	case "withdraw":
		if balance < m.Amount {
			return errors.New("insufficient funds")
		}
		balance -= m.Amount
	default:
		return errors.New("unknown type")
	}

	_, err = tx.Exec(ctx, `UPDATE accounts SET balance=$1 WHERE id=$2`, balance, m.AccountID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `INSERT INTO applied_messages(message_id) VALUES ($1)`, m.MessageID)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Write to Mongo ledger (non-transactional but acceptable for event log)
	doc := bson.D{
		{Key: "message_id", Value: m.MessageID},
		{Key: "account_id", Value: m.AccountID},
		{Key: "type", Value: m.Type},
		{Key: "amount", Value: m.Amount},
		{Key: "applied_at", Value: time.Now().UTC()},
		{Key: "balance_after", Value: balance},
	}
	if _, err := ledger.InsertOne(ctx, doc); err != nil {
		return err
	}

	return nil
}
