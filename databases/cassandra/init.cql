-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS events_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE events_ks;

-- Create Tables
CREATE TABLE IF NOT EXISTS application_logs (
    log_id UUID PRIMARY KEY,
    service_name TEXT,
    log_level TEXT,
    message TEXT,
    log_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_events (
    event_id UUID,
    user_id INT,
    event_type TEXT,
    event_data TEXT,
    event_timestamp TIMESTAMP,
    PRIMARY KEY (user_id, event_timestamp)
) WITH CLUSTERING ORDER BY (event_timestamp DESC);

-- Seed Data (Note: UUIDs generated manually for example)
INSERT INTO application_logs (log_id, service_name, log_level, message, log_timestamp)
VALUES (uuid(), 'order-service', 'INFO', 'Order #1 created successfully', '2023-10-01 10:00:00');

INSERT INTO application_logs (log_id, service_name, log_level, message, log_timestamp)
VALUES (uuid(), 'payment-service', 'ERROR', 'Failed to process payment for Order #3', '2023-10-03 14:15:00');

INSERT INTO user_events (event_id, user_id, event_type, event_data, event_timestamp)
VALUES (uuid(), 1, 'login', '{"ip": "192.168.1.1"}', '2023-10-01 08:55:00');

INSERT INTO user_events (event_id, user_id, event_type, event_data, event_timestamp)
VALUES (uuid(), 1, 'checkout_start', '{"cart_id": 501}', '2023-10-01 09:55:00');
