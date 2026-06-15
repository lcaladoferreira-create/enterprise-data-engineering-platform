-- DDL for MySQL source database (billing_db)

CREATE TABLE IF NOT EXISTS invoices (
    invoice_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    invoice_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    due_date DATETIME,
    amount DECIMAL(10, 2) NOT NULL,
    status ENUM('unpaid', 'paid', 'cancelled') DEFAULT 'unpaid',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,
    invoice_id INT NOT NULL,
    payment_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(50),
    transaction_id VARCHAR(100) UNIQUE,
    FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id)
);
