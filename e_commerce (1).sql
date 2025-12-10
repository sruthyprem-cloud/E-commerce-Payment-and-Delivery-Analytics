USE e_commerce;

CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE olist_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(50),
    seller_state VARCHAR(5)
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_sellers_dataset.csv'
INTO TABLE olist_sellers
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE olist_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_products_dataset.csv'
INTO TABLE olist_products
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    product_id,
    product_category_name,
    @product_name_length,
    @product_description_length,
    @product_photos_qty,
    @product_weight_g,
    @product_length_cm,
    @product_height_cm,
    @product_width_cm
)
SET 
    product_name_length = NULLIF(@product_name_length, ''),
    product_description_length = NULLIF(@product_description_length, ''),
    product_photos_qty = NULLIF(@product_photos_qty, ''),
    product_weight_g = NULLIF(@product_weight_g, ''),
    product_length_cm = NULLIF(@product_length_cm, ''),
    product_height_cm = NULLIF(@product_height_cm, ''),
    product_width_cm = NULLIF(@product_width_cm, '');
    
CREATE TABLE order_reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title VARCHAR(255),
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_order_reviews_dataset.csv'
IGNORE
INTO TABLE order_reviews
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2)
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_order_payments_dataset.csv'
INTO TABLE order_payments
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_order_items_dataset.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    order_id,
    order_item_id,
    product_id,
    seller_id,
    @shipping_limit_date,
    @price,
    @freight_value
)
SET
    shipping_limit_date = NULLIF(@shipping_limit_date, ''),
    price               = NULLIF(@price, ''),
    freight_value       = NULLIF(@freight_value, '');

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    order_id,
    customer_id,
    order_status,
    @order_purchase_timestamp,
    @order_approved_at,
    @order_delivered_carrier_date,
    @order_delivered_customer_date,
    @order_estimated_delivery_date
)
SET
    order_purchase_timestamp      = NULLIF(@order_purchase_timestamp, ''),
    order_approved_at             = NULLIF(@order_approved_at, ''),
    order_delivered_carrier_date  = NULLIF(@order_delivered_carrier_date, ''),
    order_delivered_customer_date = NULLIF(@order_delivered_customer_date, ''),
    order_estimated_delivery_date = NULLIF(@order_estimated_delivery_date, '');
   
   
CREATE TABLE geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat DECIMAL(10,8),
    geolocation_lng DECIMAL(11,8),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(5)
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_geolocation_dataset.csv'
INTO TABLE geolocation
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(5)
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @customer_id,
    @customer_unique_id,
    @zip_prefix,
    @city,
    @state
)
SET
    customer_id = TRIM(@customer_id),
    customer_unique_id = TRIM(@customer_unique_id),
    customer_zip_code_prefix = TRIM(@zip_prefix),
    customer_city = TRIM(@city),
    customer_state = TRIM(TRAILING '\r' FROM @state);



#1) KPI 1: Weekday vs Weekend Payment Statistics

#Classify each order as Weekday or Weekend
SELECT 
    order_id,
    order_purchase_timestamp,
    CASE 
        WHEN DAYOFWEEK(order_purchase_timestamp) IN (1,7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM orders;

#Combined KPI Table (Total, Avg, Payment Type Count)
SELECT 
    day_type,
    COUNT(*) AS number_of_payments,
    SUM(payment_value) AS total_payment_value,
    AVG(payment_value) AS average_payment_value
FROM (
    SELECT 
        o.order_id,
        CASE 
            WHEN DAYOFWEEK(o.order_purchase_timestamp) IN (1,7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        p.payment_value
    FROM orders o
    JOIN order_payments p ON o.order_id = p.order_id
) AS t
GROUP BY day_type;

#Breakdown of payment types (credit, debit, boleto, etc.)
SELECT
    day_type,
    payment_type,
    COUNT(*) AS total_transactions,
    SUM(payment_value) AS total_value
FROM (
    SELECT 
        o.order_id,
        CASE 
            WHEN DAYOFWEEK(o.order_purchase_timestamp) IN (1,7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        p.payment_type,
        p.payment_value
    FROM orders o
    JOIN order_payments p ON o.order_id = p.order_id
) AS t
GROUP BY day_type, payment_type
ORDER BY day_type, total_transactions DESC;


#2) KPI 2: Count of Orders with Review Score 5 and Payment Type as Credit Card

#Count of Orders with Review Score 5 + Credit Card
SELECT 
    COUNT(DISTINCT r.order_id) AS total_orders_with_score5_creditcard
FROM order_reviews r
JOIN order_payments p ON r.order_id = p.order_id
WHERE r.review_score = 5
  AND p.payment_type = 'credit_card';

#Correlation Insight — % of 5-Star Orders Paid by Credit Card
SELECT 
    payment_type,
    COUNT(*) AS total_orders,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage_share
FROM (
    SELECT 
        r.order_id,
        r.review_score,
        p.payment_type
    FROM order_reviews r
    JOIN order_payments p ON r.order_id = p.order_id
    WHERE r.review_score = 5
) AS t
GROUP BY payment_type
ORDER BY total_orders DESC;

#3) KPI 3: Average Delivery Time for Pet Shop Products

#Average Delivery Time for Pet Shop Products
SELECT
    o.order_id,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_days
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN olist_products p ON oi.product_id = p.product_id
WHERE p.product_category_name = 'pet_shop';


#Calculate the Average Delivery Time
SELECT 
    AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)) 
        AS avg_delivery_days_pet_shop
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN olist_products p ON oi.product_id = p.product_id
WHERE p.product_category_name = 'pet_shop'
  AND o.order_delivered_customer_date IS NOT NULL;

#4) KPI 4: Average Order Price and Payment Amount for Customers in São Paulo

#Order Price & Payment Value (SP Customers)
SELECT 
    AVG(oi.price) AS avg_order_price_sp,
    AVG(p.payment_value) AS avg_payment_value_sp
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN order_payments p ON o.order_id = p.order_id
WHERE c.customer_state = 'SP';

#5) KPI 5: Relationship Between Shipping Days and Review Scores

SELECT 
    r.review_score,
    AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)) 
        AS avg_shipping_days
FROM orders o
JOIN order_reviews r ON o.order_id = r.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY r.review_score
ORDER BY r.review_score;

