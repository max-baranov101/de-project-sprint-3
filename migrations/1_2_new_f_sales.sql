-- Учет и обновление данных заказов в витрине `mart.f_sales` по новым статусам `shipped` и `refunded`.


-- Добавление нового столбца статуса в staging.user_order_log
-- ALTER TABLE staging.user_order_log
-- ADD column status varchar(30) NULL DEFAULT 'shipped';

-- Предварительное обновление статуса в staging.user_order_log для обработки записей без статуса
-- UPDATE staging.user_order_log
-- SET status = 'shipped'
-- WHERE status IS NULL;

-- Добавление нового столбца статуса в mart.f_sales
-- ALTER TABLE mart.f_sales
-- ADD column status varchar(30) NOT NULL DEFAULT 'shipped';

-- Удаление возможных дубликатов за день '{{ds}}' перед вставкой новых данных
DELETE FROM
  mart.f_sales
WHERE
  exists(
    SELECT
      1
    FROM
      mart.d_calendar AS dc
    WHERE
      dc.date_id = mart.f_sales.date_id
      AND dc.date_actual = '{{ds}}'
  );

-- Вставка новых данных в витрину `mart.f_sales` по новым статусам `shipped` и `refunded`  
insert into
  mart.f_sales (
    date_id,
    item_id,
    customer_id,
    city_id,
    quantity,
    payment_amount,
    status
  )
select
  dc.date_id,
  uol.item_id,
  uol.customer_id,
  uol.city_id,
  uol.quantity,
  case
    when status = 'refunded' then payment_amount * -1
    else payment_amount
  end as payment_amount,
  uol.status
from
  staging.user_order_log as uol
  left join mart.d_calendar as dc on uol.date_time :: date = dc.date_actual
where
  uol.date_time :: date = '{{ds}}';