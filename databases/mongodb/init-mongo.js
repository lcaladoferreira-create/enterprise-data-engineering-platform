db = db.getSiblingDB('catalog_db');

db.products.insertMany([
  {
    product_id: 101,
    name: "Smartphone X",
    category: "Electronics",
    brand: "TechCo",
    price: 100.00,
    specs: { screen: "6.1 inch", battery: "4000mAh", storage: "128GB" },
    stock: 50
  },
  {
    product_id: 102,
    name: "Wireless Earbuds",
    category: "Electronics",
    brand: "AudioPro",
    price: 25.25,
    specs: { type: "In-ear", battery: "20 hours", noise_cancelling: true },
    stock: 150
  },
  {
    product_id: 103,
    name: "Mechanical Keyboard",
    category: "Accessories",
    brand: "TypeFast",
    price: 85.00,
    specs: { switches: "Blue", layout: "Tenkeyless", rgb: true },
    stock: 75
  },
  {
    product_id: 104,
    name: "Ergonomic Chair",
    category: "Furniture",
    brand: "ComfortMax",
    price: 70.00,
    specs: { material: "Mesh", weight_capacity: "150kg", adjustable_armrests: true },
    stock: 20
  }
]);

db.user_activity.insertMany([
  {
    user_id: 1,
    activity_type: "view_product",
    product_id: 101,
    timestamp: new Date("2023-10-01T09:00:00Z"),
    platform: "mobile"
  },
  {
    user_id: 2,
    activity_type: "add_to_cart",
    product_id: 103,
    timestamp: new Date("2023-10-02T11:00:00Z"),
    platform: "web"
  }
]);
