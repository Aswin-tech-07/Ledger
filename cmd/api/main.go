package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Server struct {
	DB        *pgxpool.Pool
	QueueName string
	AMQP      *amqp.Connection
}

type CreateAccountRequest struct {
	Owner          string `json:"owner"`
	InitialBalance int64  `json:"initial_balance"`
}

type CreateAccountResponse struct {
	ID      string `json:"id"`
	Owner   string `json:"owner"`
	Balance int64  `json:"balance"`
}

type TransactionType string

const (
	Deposit  TransactionType = "deposit"
	Withdraw TransactionType = "withdraw"
)

type EnqueueTransactionRequest struct {
	AccountID string          `json:"account_id"`
	Type      TransactionType `json:"type"`
	Amount    int64           `json:"amount"`          // cents; must be > 0
	IdemKey   string          `json:"idempotency_key"` // optional; UUID recommended
}

type LedgerQueryResponse struct {
	Transactions any `json:"transactions"`
}

func main() {
	ctx := context.Background()
	dbURL := getenv("DATABASE_URL", "postgres://ledger:password@localhost:5432/ledger")
	queue := getenv("QUEUE_NAME", "transactions")
	port := getenv("PORT", "8080")
	amqpURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")

	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	amqConn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("amqp: %v", err)
	}
	ch, err := amqConn.Channel()
	if err != nil {
		log.Fatalf("amqp channel: %v", err)
	}
	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("queue declare: %v", err)
	}
	_ = ch.Close()

	srv := &Server{DB: db, QueueName: queue, AMQP: amqConn}

	r := chi.NewRouter()
	r.Post("/accounts", srv.handleCreateAccount)
	r.Post("/transactions", srv.handleEnqueueTransaction)
	r.Get("/accounts/{id}/ledger", srv.handleGetLedger)

	log.Printf("api listening on :%s", port)
	http.ListenAndServe(":"+port, r)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func (s *Server) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	var req CreateAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Owner == "" || req.InitialBalance < 0 {
		http.Error(w, "invalid owner or initial_balance", http.StatusBadRequest)
		return
	}
	id := uuid.New()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, err := s.DB.Exec(ctx, `INSERT INTO accounts(id, owner, balance) VALUES ($1,$2,$3)`, id, req.Owner, req.InitialBalance)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	resp := CreateAccountResponse{ID: id.String(), Owner: req.Owner, Balance: req.InitialBalance}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleEnqueueTransaction(w http.ResponseWriter, r *http.Request) {
	var req EnqueueTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(req.AccountID); err != nil {
		http.Error(w, "invalid account_id", http.StatusBadRequest)
		return
	}
	if req.Amount <= 0 || (req.Type != Deposit && req.Type != Withdraw) {
		http.Error(w, "invalid type or amount", http.StatusBadRequest)
		return
	}
	messageID := req.IdemKey
	if messageID == "" {
		messageID = uuid.NewString()
	}

	payload := map[string]any{
		"message_id":  messageID,
		"account_id":  req.AccountID,
		"type":        string(req.Type),
		"amount":      req.Amount,
		"requestedAt": time.Now().UTC(),
	}
	body, _ := json.Marshal(payload)

	ch, err := s.AMQP.Channel()
	if err != nil {
		http.Error(w, "queue channel error", http.StatusInternalServerError)
		return
	}
	defer ch.Close()

	err = ch.PublishWithContext(r.Context(), "", s.QueueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Body:         body,
	})
	if err != nil {
		http.Error(w, "publish error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "enqueued", "message_id": messageID})
}

func (s *Server) handleGetLedger(w http.ResponseWriter, r *http.Request) {
	// The ledger is stored in MongoDB; this API will query it via HTTP to the worker's shared code.
	// To keep app simple for the take-home, we expose this endpoint but implement the query here.
	accountID := chi.URLParam(r, "id")
	if _, err := uuid.Parse(accountID); err != nil {
		http.Error(w, "invalid account id", http.StatusBadRequest)
		return
	}
	mongoURL := getenv("MONGO_URL", "mongodb://localhost:27017")
	mongoDB := getenv("MONGO_DB", "ledger")

	records, err := QueryLedger(r.Context(), mongoURL, mongoDB, accountID, 100, 0)
	if err != nil {
		http.Error(w, "ledger query error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, LedgerQueryResponse{Transactions: records})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// -------- Shared Mongo ledger query (used by API) --------
type LedgerRecord struct {
	MessageID string    `bson:"message_id" json:"message_id"`
	AccountID string    `bson:"account_id" json:"account_id"`
	Type      string    `bson:"type" json:"type"`
	Amount    int64     `bson:"amount" json:"amount"`
	AppliedAt time.Time `bson:"applied_at" json:"applied_at"`
	Balance   int64     `bson:"balance_after" json:"balance_after"`
}

func QueryLedger(ctx context.Context, mongoURL, dbName, accountID string, limit, skip int64) ([]LedgerRecord, error) {
	// Local import to avoid a full shared package for brevity
	type filterT = map[string]any

	client, err := connectMongo(ctx, mongoURL)
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(ctx)
	coll := client.Database(dbName).Collection("ledger")
	cur, err := coll.Find(ctx, filterT{"account_id": accountID}, nil)
	if err != nil {
		return nil, err
	}
	var out []LedgerRecord
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}
