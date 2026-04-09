CREATE TABLE IF NOT EXISTS leopard_string_table (
    id         INT UNSIGNED    NOT NULL AUTO_INCREMENT PRIMARY KEY,
    store_id   VARCHAR(26)     NOT NULL,
    value      TEXT            NOT NULL,
    created_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_store_value (store_id, value(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
