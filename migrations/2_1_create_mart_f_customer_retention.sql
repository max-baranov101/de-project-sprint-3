-- Создание витрины mart.f_customer_retention:
CREATE TABLE IF NOT EXISTS mart.f_customer_retention(
	new_customers_count BIGINT NULL,
	returning_customers_count BIGINT NULL,
	refunded_customer_count BIGINT NULL,
	period_name VARCHAR(10) DEFAULT 'weekly',
	period_id INT NOT NULL,
	item_id BIGINT	NOT null,      
	new_customers_revenue NUMERIC(14, 2) NULL,
	returning_customers_revenue NUMERIC(14, 2) NULL,
	customers_refunded INT NULL
);
