-- Accounts table stores the current balance; amounts are in cents (int64) for accuracy.
CREATE TABLE IF NOT EXISTS accounts (
  id UUID PRIMARY KEY,
  owner TEXT NOT NULL,
  balance BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotency to prevent duplicate transaction application
CREATE TABLE IF NOT EXISTS applied_messages (
  message_id UUID PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
