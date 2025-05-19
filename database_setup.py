import mysql.connector
from faker import Faker
import random
from datetime import datetime

# --- Database Connection Details ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',      # Replace with your MySQL username
    'password': 'Sql@#&50490', # Replace with your MySQL password
    'database': 'semantic_catalog_db'
}

# Initialize Faker
fake = Faker()

def create_database_and_tables():
    """Creates the database and the tables if they don't already exist."""
    # Use specific variable names within this function to avoid confusion
    _db_conn = None
    _db_cursor = None
    try:
        # Connect to MySQL server (without specifying a database initially)
        temp_conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        temp_cursor = temp_conn.cursor()
        temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        print(f"Database '{DB_CONFIG['database']}' created or already exists.")
        temp_cursor.close()
        temp_conn.close()

        # Connect to the specific database
        _db_conn = mysql.connector.connect(**DB_CONFIG)
        _db_cursor = _db_conn.cursor()

        # --- Create Tables ---
        tables = {
            "Customers": """
                CREATE TABLE IF NOT EXISTS Customers (
                    customer_id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) UNIQUE,
                    phone VARCHAR(50),
                    address TEXT
                )
            """,
            "Products": """
                CREATE TABLE IF NOT EXISTS Products (
                    product_id INT AUTO_INCREMENT PRIMARY KEY,
                    product_name VARCHAR(255) NOT NULL,
                    description TEXT,
                    price DECIMAL(10, 2) NOT NULL,
                    category VARCHAR(100)
                )
            """,
            "Orders": """
                CREATE TABLE IF NOT EXISTS Orders (
                    order_id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_id INT,
                    order_date DATE,
                    total_amount DECIMAL(10, 2),
                    status VARCHAR(50),
                    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
                )
            """,
            "Order_Items": """
                CREATE TABLE IF NOT EXISTS Order_Items (
                    order_item_id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id INT,
                    product_id INT,
                    quantity INT,
                    unit_price DECIMAL(10, 2),
                    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
                    FOREIGN KEY (product_id) REFERENCES Products(product_id)
                )
            """,
            "enriched_metadata": """
                CREATE TABLE IF NOT EXISTS enriched_metadata (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    object_type VARCHAR(50) NOT NULL, -- 'table' or 'column'
                    object_name VARCHAR(255) NOT NULL, -- original table/column name
                    parent_table_name VARCHAR(255), -- NULL for tables, parent table name for columns
                    technical_metadata JSON, -- Store the extracted data here
                    semantic_description TEXT, -- LLM-generated description
                    tags JSON, -- LLM-generated tags (stored as a JSON array of strings)
                    llm_model_used VARCHAR(100),
                    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }

        for table_name, create_statement in tables.items():
            _db_cursor.execute(create_statement)
            print(f"Table '{table_name}' created or already exists.")

        _db_conn.commit()
        print("All tables created successfully.")
        return _db_conn, _db_cursor # Return the active connection and cursor

    except mysql.connector.Error as err:
        print(f"Error creating database/tables: {err}")
        if _db_cursor: # If cursor was created before error
            try: _db_cursor.close()
            except mysql.connector.Error: pass # Ignore errors on close during error handling
        if _db_conn and _db_conn.is_connected(): # If connection was established before error
            try: _db_conn.rollback()
            except mysql.connector.Error: pass
            try: _db_conn.close()
            except mysql.connector.Error: pass
        return None, None

def populate_dummy_data(conn, cursor, num_customers=75, num_products=50, num_orders=100):
    """Populates the tables with dummy data."""
    if not conn or not cursor or not conn.is_connected():
        print("Database connection is not active. Cannot populate data.")
        return

    try:
        # --- Populate Customers ---
        customers_data = []
        for _ in range(num_customers):
            customers_data.append((
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.phone_number(),
                fake.address()
            ))
        cursor.executemany(
            "INSERT INTO Customers (first_name, last_name, email, phone, address) VALUES (%s, %s, %s, %s, %s)",
            customers_data
        )
        print(f"Populated {cursor.rowcount} rows into Customers.")

        # --- Populate Products ---
        products_data = []
        categories = ['Electronics', 'Books', 'Clothing', 'Home Goods', 'Sports', 'Toys']
        for _ in range(num_products):
            products_data.append((
                fake.catch_phrase() + " " + random.choice(['Gadget', 'Tool', 'Accessory', 'Device']), # More varied product names
                fake.text(max_nb_chars=200),
                round(random.uniform(5.0, 500.0), 2),
                random.choice(categories)
            ))
        cursor.executemany(
            "INSERT INTO Products (product_name, description, price, category) VALUES (%s, %s, %s, %s)",
            products_data
        )
        print(f"Populated {cursor.rowcount} rows into Products.")

        # Fetch all customer_ids and product_ids to ensure valid foreign keys
        cursor.execute("SELECT customer_id FROM Customers")
        customer_ids = [item[0] for item in cursor.fetchall()]
        cursor.execute("SELECT product_id FROM Products")
        product_ids = [item[0] for item in cursor.fetchall()]

        if not customer_ids or not product_ids:
            print("Cannot populate Orders or Order_Items due to missing customer or product data.")
            return

        # --- Populate Orders ---
        orders_data = []
        order_statuses = ['Pending', 'Shipped', 'Delivered', 'Cancelled', 'Processing']
        for _ in range(num_orders):
            order_date = fake.date_between(start_date='-2y', end_date='today')
            orders_data.append((
                random.choice(customer_ids),
                order_date,
                0.0,  # Placeholder for total_amount, will be updated later
                random.choice(order_statuses)
            ))
        cursor.executemany(
            "INSERT INTO Orders (customer_id, order_date, total_amount, status) VALUES (%s, %s, %s, %s)",
            orders_data
        )
        print(f"Populated {cursor.rowcount} rows into Orders (initial).")

        # --- Populate Order_Items and Update Order Total ---
        cursor.execute("SELECT order_id FROM Orders")
        order_ids = [item[0] for item in cursor.fetchall()]
        order_items_data = []
        order_totals = {order_id: 0.0 for order_id in order_ids}

        for order_id in order_ids:
            num_items_in_order = random.randint(1, 5)
            for _ in range(num_items_in_order):
                product_id = random.choice(product_ids)
                cursor.execute("SELECT price FROM Products WHERE product_id = %s", (product_id,))
                product_price_result = cursor.fetchone()
                if product_price_result:
                    unit_price = product_price_result[0]
                    quantity = random.randint(1, 5)
                    order_items_data.append((
                        order_id,
                        product_id,
                        quantity,
                        unit_price
                    ))
                    order_totals[order_id] += float(quantity * unit_price)

        cursor.executemany(
            "INSERT INTO Order_Items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
            order_items_data
        )
        print(f"Populated {cursor.rowcount} rows into Order_Items.")

        # Update total_amount in Orders table
        for order_id, total in order_totals.items():
            cursor.execute(
                "UPDATE Orders SET total_amount = %s WHERE order_id = %s",
                (round(total, 2), order_id)
            )
        print("Updated total_amount in Orders table.")

        conn.commit()
        print("Dummy data populated successfully.")

    except mysql.connector.Error as err:
        print(f"Error populating data: {err}")
        if conn.is_connected():
            conn.rollback()
    except Exception as e:
        print(f"An unexpected error occurred during data population: {e}")
        if conn.is_connected():
            conn.rollback()

if __name__ == "__main__":
    print("Starting database setup...")
    # 1. Create Database and Tables
    conn, cursor = create_database_and_tables()

    if conn and cursor:
        try:
            # Check if tables are empty before populating
            cursor.execute("SELECT COUNT(*) FROM Customers")
            if cursor.fetchone()[0] == 0:
                print("Tables are empty, proceeding to populate dummy data.")
                # 2. Populate Dummy Data
                populate_dummy_data(conn, cursor, num_customers=75, num_products=50, num_orders=100)
            else:
                print("Tables already contain data. Skipping dummy data population.")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
                print("Database connection closed.")
    else:
        print("Database setup failed. Cannot proceed with data population.")

    print("Database setup script finished.")
