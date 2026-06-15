import os
import json
import time

def seed_raw_data():
    """
    Initializes raw JSON files in the data/raw directory to kick off the pipeline.
    """
    print("Initializing raw data for ingestion...")

    base_path = "data/raw"
    entities = {
        "customers": [
            {"customer_id": 1, "first_name": "John", "last_name": "Doe", "email": "john.doe@example.com", "city": "New York", "country": "USA"},
            {"customer_id": 2, "first_name": "Jane", "last_name": "Smith", "email": "jane.smith@example.com", "city": "Los Angeles", "country": "USA"}
        ],
        "orders": [
            {"order_id": 1, "customer_id": 1, "order_date": "2023-10-01 10:00:00", "status": "completed", "total_amount": 150.50},
            {"order_id": 2, "customer_id": 2, "order_date": "2023-10-02 11:30:00", "status": "shipped", "total_amount": 85.00}
        ],
        "products": [
            {"product_id": 101, "name": "Smartphone X", "category": "Electronics", "brand": "TechCo", "price": 100.00},
            {"product_id": 102, "name": "Wireless Earbuds", "category": "Electronics", "brand": "AudioPro", "price": 25.25}
        ],
        "order_items": [
            {"order_item_id": 1, "order_id": 1, "product_id": 101, "quantity": 1, "unit_price": 100.00},
            {"order_item_id": 2, "order_id": 1, "product_id": 102, "quantity": 2, "unit_price": 25.25}
        ],
        "payments": [
            {"payment_id": 1, "invoice_id": 1, "payment_date": "2023-10-01 10:10:00", "amount": 150.50, "payment_method": "credit_card", "transaction_id": "TXN_001"}
        ],
        "invoices": [
            {"invoice_id": 1, "order_id": 1, "invoice_date": "2023-10-01 10:05:00", "due_date": "2023-10-15 10:05:00", "amount": 150.50, "status": "paid"}
        ]
    }

    for entity, data in entities.items():
        entity_path = os.path.join(base_path, entity)
        os.makedirs(entity_path, exist_ok=True)

        file_path = os.path.join(entity_path, f"{entity}_{int(time.time())}.json")
        with open(file_path, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")

        print(f"Created {file_path}")

if __name__ == "__main__":
    seed_raw_data()
