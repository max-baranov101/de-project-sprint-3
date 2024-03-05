-- Расчет количества и выручки от новых клиентов
DROP MATERIALIZED VIEW IF EXISTS cte1_view; -- исправлено на мат. вью
CREATE MATERIALIZED VIEW cte1_view AS -- исправлено на мат. вью
WITH cte1 AS ( 
	SELECT 
		c.week_of_year,
		FS.item_id AS item_new,
		FS.customer_Id,
		sum(payment_amount) AS payment_amount
	FROM mart.f_sales fs
		JOIN mart.d_calendar c ON FS.date_id = c.date_id
	WHERE 
		quantity > 0
		AND customer_id IN (SELECT customer_id 
							FROM mart.f_sales 
							WHERE quantity > 0 
							GROUP BY customer_id HAVING count(*)=1)
	GROUP BY 	
		c.week_of_year,
		FS.item_id,
		FS.customer_Id
)
SELECT 
	week_of_year,
	item_new,
	count(DISTINCT customer_id) AS new_customers_count, 
	sum(payment_amount) AS new_customers_revenue
FROM cte1
GROUP BY week_of_year,item_new;

-- Расчет количества и выручки от вернувшихся клиентов
DROP MATERIALIZED VIEW IF EXISTS cte2_view; -- исправлено на мат. вью
CREATE MATERIALIZED VIEW cte2_view AS -- исправлено на мат. вью
WITH cte2 AS ( 
	SELECT 
		c.week_of_year,
		FS.item_id AS item_returning, 
		FS.customer_id,
		sum(FS.payment_amount) AS payment_amount
	FROM mart.f_sales fs
		JOIN mart.d_calendar c ON FS.date_id = c.date_id
		FULL JOIN (SELECT customer_id
				   FROM mart.f_sales
				   WHERE quantity > 0
				   GROUP BY customer_id
				   HAVING count(*)>1) f ON f.customer_id = FS.customer_id
	WHERE 
		quantity > 0
	GROUP BY 	
		c.week_of_year,
		FS.item_id,
		FS.customer_id
)
SELECT	week_of_year, 
	    item_returning,
		count(DISTINCT customer_id) AS returning_customers_count, 
		sum(payment_amount) AS returning_customers_revenue  
FROM cte2
GROUP BY week_of_year, item_returning;

-- Расчет количества клиентов, оформивших возврат, и сумму возвратов
DROP MATERIALIZED VIEW IF EXISTS cte3_view; -- исправлено на мат. вью
CREATE MATERIALIZED VIEW cte3_view AS -- исправлено на мат. вью
WITH cte3 AS 
( 
	SELECT 
		c.week_of_year,
		FS.item_id AS item_refunding, 
		FS.customer_id,
		sum(FS.payment_amount) AS payment_amount
	FROM mart.f_sales fs
		JOIN mart.d_calendar c ON FS.date_id = c.date_id
		FULL JOIN (SELECT customer_id
				   FROM mart.f_sales
				   WHERE quantity < 0
				   GROUP BY customer_id) f ON f.customer_id = FS.customer_id
	WHERE 
		quantity < 0
	GROUP BY 	
		c.week_of_year,
		FS.item_id,
		FS.customer_id
)
SELECT	week_of_year, 
	    item_refunding,
		count(DISTINCT customer_id) AS refunded_customer_count, 
		count(customer_id) AS customers_refunded  
FROM cte3
GROUP BY week_of_year, item_refunding;

-- Наполнение витрины
INSERT INTO mart.f_customer_retention (
	new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	period_id,
	item_id,      
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded
)
SELECT	c1.new_customers_count,
		c2.returning_customers_count,
		c3.refunded_customer_count,
		c1.week_of_year AS period_id,
		coalesce(c1.item_new, c2.item_returning, c3.item_refunding) AS item_id,
		c1.new_customers_revenue,
		c2.returning_customers_revenue,
		c3.customers_refunded
	FROM cte1_view c1
		-- учтена возможность отсутствия соответствия в c1 или c2 через COALESCE
		FULL JOIN cte2_view c2 ON c1.week_of_year = c2.week_of_year AND c1.item_new = c2.item_returning
		FULL JOIN cte3_view c3 ON coalesce(c1.week_of_year, c2.week_of_year) = c3.week_of_year AND coalesce(c1.item_new, c2.item_returning) = c3.item_refunding

-- Удаление возможных дубликатов за неделю перед вставкой новых данных
ON CONFLICT (period_id, item_id) DO UPDATE SET
  new_customers_count = EXCLUDED.new_customers_count,
  returning_customers_count = EXCLUDED.returning_customers_count,
  refunded_customer_count = EXCLUDED.refunded_customer_count,
  new_customers_revenue = EXCLUDED.new_customers_revenue,
  returning_customers_revenue = EXCLUDED.returning_customers_revenue,
  customers_refunded = EXCLUDED.customers_refunded;
