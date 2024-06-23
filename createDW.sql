--  Date Dimension Table
CREATE TABLE date_dimension (
    date_id INT PRIMARY KEY,
    order_date DATE
);

--  Order Dimension Table
CREATE TABLE order_dimension (
    order_id INT PRIMARY KEY
);

-- Create Customer Dimension Table
CREATE TABLE customer_dimension (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    gender VARCHAR(10)

);

--  Product Dimension Table
CREATE TABLE product_dimension (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    product_price DECIMAL(10, 2)
);

--  Store Dimension Table
CREATE TABLE store_dimension (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(255)
);

--  Supplier Dimension Table
CREATE TABLE supplier_dimension (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(255)
);

--  Sales Fact Table
CREATE TABLE sales_fact (
    transaction_id INT PRIMARY KEY,
    date_id INT,
    order_id INT,
    customer_id INT,
    product_id INT,
    store_id INT,
    supplier_id INT,
    sale DECIMAL(10,2),
    
    FOREIGN KEY (date_id) REFERENCES date_dimension(date_id),
    FOREIGN KEY (order_id) REFERENCES order_dimension(order_id),
    FOREIGN KEY (customer_id) REFERENCES customer_dimension(customer_id),
    FOREIGN KEY (product_id) REFERENCES product_dimension(product_id),
    FOREIGN KEY (store_id) REFERENCES store_dimension(store_id),
    FOREIGN KEY (supplier_id) REFERENCES supplier_dimension(supplier_id)
);

