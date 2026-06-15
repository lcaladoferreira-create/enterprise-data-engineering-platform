-- Seed data for MySQL source database

INSERT INTO invoices (order_id, invoice_date, due_date, amount, status)
VALUES
(1, '2023-10-01 10:05:00', '2023-10-15 10:05:00', 150.50, 'paid'),
(2, '2023-10-02 11:35:00', '2023-10-16 11:35:00', 85.00, 'unpaid'),
(3, '2023-10-03 14:20:00', '2023-10-17 14:20:00', 210.00, 'unpaid'),
(4, '2023-10-04 09:50:00', '2023-10-18 09:50:00', 45.25, 'paid');

INSERT INTO payments (invoice_id, payment_date, amount, payment_method, transaction_id)
VALUES
(1, '2023-10-01 10:10:00', 150.50, 'credit_card', 'TXN_001'),
(4, '2023-10-04 09:55:00', 45.25, 'paypal', 'TXN_002');
