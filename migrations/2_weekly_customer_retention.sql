-- Наполнение витрины `mart.f_customer_retention` данными по «возвращаемости клиентов» в разрезе недель

-- Создание витрины mart.f_customer_retention:
CREATE TABLE IF NOT EXISTS mart.f_customer_retention(
  new_customers_count BIGINT,
  returning_customers_count BIGINT,
  refunded_customer_count BIGINT,
  period_name VARCHAR(20),
  period_id INT,
  item_id BIGINT,
  new_customers_revenue NUMERIC(14, 2),
  returning_customers_revenue NUMERIC(14, 2),
  customers_refunded INT
);

-- Удаление устаревших или уже обработанных записей из витрины
DELETE FROM
  mart.f_customer_retention
WHERE
  period_id IN (
    SELECT
      date_id
    FROM
      staging.user_order_log AS uol
      LEFT JOIN mart.d_calendar AS dc ON uol.date_time :: DATE = dc.date_actual
    WHERE
      uol.date_time :: DATE = '{{ds}}'
  );