CREATE TABLE message(
    message_seq serial PRIMARY KEY,
    guid text,
    client_id text,
    message_id int,
    topic text,
    ssid text[],
    ssid_len int,
    ttl_until int,
    qos int,
    payload text,
    created_at timestamp,
    updated_at timestamp
);

CREATE EXTENSION btree_gin;
CREATE INDEX idx_message_gin ON message USING GIN(
    message_seq,
    client_id,
    ttl_until,
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