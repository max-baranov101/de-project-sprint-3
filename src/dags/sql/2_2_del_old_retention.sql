-- Ресурсоэкономное удаление устаревших или уже обработанных записей из витрины

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

-- Полное удаление данных в витрине (но затратное по ресурсам)
-- TRUNCATE TABLE mart.f_customer_retention