-- Seed data for PostgreSQL source database

INSERT INTO customers (first_name, last_name, email, phone, address, city, country)
VALUES
('John', 'Doe', 'john.doe@example.com', '123456789', '123 Main St', 'New York', 'USA'),
('Jane', 'Smith', 'jane.smith@example.com', '987654321', '456 Oak Ave', 'Los Angeles', 'USA'),
('Alice', 'Johnson', 'alice.j@example.com', '555123456', '789 Pine Rd', 'Chicago', 'USA'),
('Bob', 'Brown', 'bob.brown@example.com', '444987654', '321 Maple Dr', 'Houston', 'USA');

INSERT INTO orders (customer_id, order_date, status, total_amount, shipping_address)
VALUES
(1, '2023-10-01 10:00:00', 'completed', 150.50, '123 Main St, New York, USA'),
(2, '2023-10-02 11:30:00', 'shipped', 85.00, '456 Oak Ave, Los Angeles, USA'),
(3, '2023-10-03 14:15:00', 'pending', 210.00, '789 Pine Rd, Chicago, USA'),
(1, '2023-10-04 09:45:00', 'completed', 45.25, '123 Main St, New York, USA');

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES
(1, 101, 1, 100.00),
(1, 102, 2, 25.25),
(2, 103, 1, 85.00),
(3, 104, 3, 70.00),
(4, 105, 1, 45.25);
