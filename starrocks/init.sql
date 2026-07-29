-- StarRocks sample data for the Flight SQL driver tests.
-- StarRocks "databases" are what Metabase sees as schemas. Three databases
-- mirror the GizmoSQL/Doris layout so the catalog/schema tests generalize.
-- catalog/schema/schema-filter tests generalize to a MySQL-dialect backend.

-- ---------------------------------------------------------------- sales ----
CREATE DATABASE IF NOT EXISTS sales;

CREATE TABLE IF NOT EXISTS sales.customers (
    customer_id INT,
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    email       VARCHAR(100),
    city        VARCHAR(50),
    country     VARCHAR(50),
    created_at  DATETIME
)
DUPLICATE KEY(customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO sales.customers VALUES
(1,'Alice','Johnson','alice@example.com','New York','USA','2024-01-15 10:30:00'),
(2,'Bruno','Meier','bruno@example.com','Berlin','Germany','2024-02-20 09:00:00'),
(3,'Chen','Wang','chen@example.com','Shanghai','China','2024-03-10 14:00:00'),
(4,'Diana','Lopez','diana@example.com','Madrid','Spain','2024-04-05 11:15:00'),
(5,'Erik','Svensson','erik@example.com','Stockholm','Sweden','2024-05-12 16:45:00');

CREATE TABLE IF NOT EXISTS sales.orders (
    order_id     INT,
    customer_id  INT,
    order_date   DATE,
    status       VARCHAR(20),
    total_amount DECIMAL(10,2)
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO sales.orders VALUES
(101,1,'2024-06-01','Delivered',129.99),
(102,2,'2024-06-03','Shipped',249.00),
(103,3,'2024-06-05','Delivered',59.90),
(104,1,'2024-06-10','Pending',899.99),
(105,4,'2024-06-12','Delivered',199.99),
(106,2,'2024-06-18','Cancelled',49.99),
(107,5,'2024-06-20','Delivered',749.99),
(108,3,'2024-06-25','Shipped',389.00);

-- ------------------------------------------------------------------- hr ----
CREATE DATABASE IF NOT EXISTS hr;

CREATE TABLE IF NOT EXISTS hr.employees (
    employee_id INT,
    name        VARCHAR(100),
    department  VARCHAR(50),
    salary      DECIMAL(10,2),
    hired_on    DATE
)
DUPLICATE KEY(employee_id)
DISTRIBUTED BY HASH(employee_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO hr.employees VALUES
(1,'Grace Hopper','Engineering',120000.00,'2021-03-01'),
(2,'Alan Kay','Engineering',115000.00,'2021-06-15'),
(3,'Radia Perlman','Networking',118000.00,'2022-01-10'),
(4,'Barbara Liskov','Research',125000.00,'2020-09-01'),
(5,'Katherine Johnson','Analytics',98000.00,'2023-02-20');

-- ------------------------------------------------------------ analytics ----
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.website_events (
    event_id   INT,
    event_type VARCHAR(40),
    event_ts   DATETIME,
    user_id    INT
)
DUPLICATE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO analytics.website_events VALUES
(1,'page_view','2024-06-01 08:00:00',1),
(2,'add_to_cart','2024-06-01 08:05:00',1),
(3,'page_view','2024-06-01 09:00:00',2),
(4,'checkout','2024-06-01 09:30:00',2),
(5,'page_view','2024-06-02 10:00:00',3);
