CREATE TABLE message(
    id serial PRIMARY KEY,
    username text,
    topic text,
    ssid_len int,
    ssid text[],
    ttl_until int,
    guid_id text,
    message_id int,
    qos int,
    payload text
);

CREATE EXTENSION btree_gin;
CREATE INDEX idx_message_gin ON message USING GIN(
    id,
    ttl_until,
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