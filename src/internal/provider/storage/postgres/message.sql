CREATE TABLE message(
    id serial PRIMARY KEY,
    message_seq bigint,
    username text,
    client_id text,
    topic text,
    ssid text[],
    ssid_len int,
    qos int,
    type int,
    payload text,
    created_at timestamp,
    updated_at timestamp
);

CREATE EXTENSION btree_gin;
CREATE INDEX idx_message_gin ON message USING GIN(
    message_seq,
    username,
    created_at,
    ssid_len,
    (ssid[0]),
    (ssid[1]),
    (ssid[2]),
    (ssid[3]),
    (ssid[4]),
    (ssid[5]),
    (ssid[6]),
    (ssid[7])
);