CREATE TABLE IF NOT EXISTS leopard_epochs (
    store_id   VARCHAR(26) PRIMARY KEY,
    epoch      BIGINT      NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
