CREATE TABLE IF NOT EXISTS leopard_string_table (
    id         SERIAL      PRIMARY KEY,
    store_id   VARCHAR(26) NOT NULL,
    value      TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (store_id, value)
);
