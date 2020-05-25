CREATE TABLE subscription(
    id serial PRIMARY KEY,
    client_id text,
    topic text,
    ssid text[],
    ssid_len int,
    created_at timestamp,
    updated_at timestamp
);

CREATE EXTENSION btree_gin;
CREATE INDEX idx_subscription_gin ON subscription USING GIN(
    client_id,
    topic,
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