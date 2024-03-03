-- существующие запросы

-- mart.d_city
insert into mart.d_city (city_id, city_name)
select city_id, city_name from staging.user_order_log
where city_id not in (select city_id from mart.d_city)
group by city_id, city_name;

-- mart.d_customer
insert into mart.d_customer (customer_id, first_name, last_name, city_id)
select customer_id, first_name, last_name, max(city_id) from staging.user_order_log
where customer_id not in (select customer_id from mart.d_customer)
group by customer_id, first_name, last_name

-- mart.d_item
insert into mart.d_item (item_id, item_name)
select item_id, item_name from staging.user_order_log
where item_id not in (select item_id from mart.d_item)
group by item_id, item_name

-- mart.f_sales
insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select dc.date_id, item_id, customer_id, city_id, quantity, payment_amount from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';