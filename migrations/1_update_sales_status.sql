-- Учет и обновление данных заказов в витрине `mart.f_sales` по новым статусам `shipped` и `refunded`.


-- Добавление нового столбца статуса в mart.f_sales
ALTER TABLE
	mart.f_sales
ADD
	column status varchar(30) NOT NULL DEFAULT 'shipped';

-- Добавление нового столбца статуса в staging.user_order_log
ALTER TABLE
	staging.user_order_log
ADD
	column status varchar(30) NULL;

-- Предварительное обновление статуса в staging.user_order_log для обработки записей без статуса
UPDATE
	staging.user_order_log
SET
	status = 'shipped'
WHERE
	status IS NULL;

-- Удаление возможных дубликатов за день '{{ds}}' перед вставкой новых данных
DELETE FROM
	mart.f_sales
WHERE
	date_id IN (
		SELECT
			dc.date_id
		FROM
			staging.user_order_log uol
			JOIN mart.d_calendar dc ON uol.date_time :: Date = dc.date_actual
		WHERE
			uol.date_time :: Date = '{{ds}}'
	);

-- Обновление данных в mart.f_sales с учетом статуса заказов
INSERT INTO
	mart.f_sales (
		date_id,
		item_id,
		customer_id,
		city_id,
		quantity,
		payment_amount,
		status
	)
SELECT
	dc.date_id,
	uol.item_id,
	uol.customer_id,
	uol.city_id,
	CASE
		WHEN uol.status = 'refunded' THEN -1 * uol.quantity
		ELSE uol.quantity
	END,
	CASE
		WHEN uol.status = 'refunded' THEN -1 * uol.payment_amount
		ELSE uol.payment_amount
	END,
	uol.status
FROM
	staging.user_order_log uol
	LEFT JOIN mart.d_calendar AS dc ON uol.date_time :: Date = dc.date_actual
WHERE
	uol.date_time :: Date = '{{ds}}' ON CONFLICT (date_id, item_id, customer_id, city_id) DO
UPDATE
SET
	quantity = EXCLUDED.quantity + mart.f_sales.quantity,
	payment_amount = EXCLUDED.payment_amount + mart.f_sales.payment_amount,
	status = EXCLUDED.status;