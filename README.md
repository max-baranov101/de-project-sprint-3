# Яндекс Практикум. Инженер данных. 3 спринт

## Общее описание

- Репозиторий предназначен для сдачи проекта 3-го спринта по ETL и автоматизации подготовки данных.
- Задача проекта: изменить процессы в пайплайне так, чтобы они соответствовали новым задачам бизнеса.
- Проект разделён на три этапа развития пайплайна.
  1. Учет и обновление данных заказов в витрине `mart.f_sales` по новым статусам `shipped` и `refunded`.
  2. Наполнение витрины `mart.f_customer_retention` данными по «возвращаемости клиентов» в разрезе недель.
  3. ОПЦИОНАЛЬНО: Проверка дубликатов в витринах `mart.f_sales` и `mart.f_customer_retention`.
- Проект выполняется постепенно с учётом получения новых вводных от заказчика. Каждый этап пайплайна учитывает удаление предыдущих данных за расчётный период.

## Структура репозитория

1) Папка `migrations` содержит SQL-скрипты миграции и обновления базы данных:
   - [0_update_mart_tables.sql](migrations/0_update_mart_tables.sql) - изначальные скрипты обновления таблицы в схеме `mart`.
   - [1_update_sales_status.sql](migrations/1_update_sales_status.sql) - скрипт учёта статусов `shipped` и `refunded` в таблице `mart.f_sales`.
   - [2_weekly_customer_retention.sql](migrations/2_weekly_customer_retention.sql) - скрипт создания таблицы `mart.f_customer_retention`.
   - [3_duplicate_check.sql](migrations/3_duplicate_check.sql) - скрипт проверки дубликатов в таблицах `mart.f_sales` и `mart.f_customer_retention`.
2) Папка `src` содержит исходные файлы проекта:
    - [src/dags](src/dags): DAG's Airflow
    - [src/report](src/report): файлы отчета, скачанные по API
    - [src/increment](src/increment): файлы инкремента отчета, скачанные по API

## Спецификация API для получения инкременов со статусом заказа

Шаги получения инкрементов с информацией о продажах и статусом заказа (shipped/refunded) используется API:

1) Инициализация формирования отчёта
2) Получение отчёта после его формирования на сервере
3) Получение данных за те даты, которые не вошли в основной отчёт

Исходные данные для запросов:

- `X-Nickname`: `baranov`
- `X-Project`: `True`
- `X-Cohort`: `23`
- `X-API-KEY`: `5f55e6c0-e9e5-4a9c-b313-63c01fc31460`

### 1. Инициализация формирования отчёта

Метод `POST /generate_report`:

```text
curl --location --request POST 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report' \
--header 'X-Nickname: baranov' \
--header 'X-Cohort: 23' \
--header 'X-Project: True' \
--header 'X-API-KEY: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460' \
--data-raw ''
```

ID задачи, в результате выполнения которой должен сформироваться отчёт:

```json
{
    "task_id": "MjAyNC0wMy0wM1QxMzoyMTozNQliYXJhbm92"
}
```

### 2. Получение отчёта после его формирования на сервере

Метод `GET /get_report`:

```text
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id=MjAyNC0wMy0wM1QxMzoyMTozNQliYXJhbm92' \
--header 'X-Nickname: baranov' \
--header 'X-Cohort: 23' \
--header 'X-Project: True' \
--header 'X-API-KEY: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
```

- Пока отчёт будет формироваться, будет возвращаться статус `RUNNING`.
- Если отчёт сформирован, то метод вернёт статус `SUCCESS`, `report_id` и ссылки на скачивание файлов из параметра `s3_path`:
  - [customer_research.csv](src/report/customer_research.csv)
  - [user_order_log.csv](src/report/user_order_log.csv)
  - [user_activity_log.csv](src/report/user_activity_log.csv)
  - [price_log.csv](src/report/price_log.csv)

```json
{
    "task_id": "MjAyNC0wMy0wM1QxMzoyMTozNQliYXJhbm92",
    "status": "SUCCESS",
    "data": {
        "report_id": "TWpBeU5DMHdNeTB3TTFReE16b3lNVG96TlFsaVlYSmhibTky",
        "start_day": "2024-02-02 00:00:00",
        "end_date": "2024-02-24 00:00:00",
        "s3_path": {
            "customer_research": "https://storage.yandexcloud.net/s3-sprint3/cohort_23/baranov/project/TWpBeU5DMHdNeTB3TTFReE16b3lNVG96TlFsaVlYSmhibTky/customer_research.csv",
            "user_order_log": "https://storage.yandexcloud.net/s3-sprint3/cohort_23/baranov/project/TWpBeU5DMHdNeTB3TTFReE16b3lNVG96TlFsaVlYSmhibTky/user_order_log.csv",
            "user_activity_log": "https://storage.yandexcloud.net/s3-sprint3/cohort_23/baranov/project/TWpBeU5DMHdNeTB3TTFReE16b3lNVG96TlFsaVlYSmhibTky/user_activity_log.csv",
            "price_log": "https://storage.yandexcloud.net/s3-sprint3/cohort_23/baranov/project/TWpBeU5DMHdNeTB3TTFReE16b3lNVG96TlFsaVlYSmhibTky/price_log.csv"
        }
    }
}
```

Файлы также можно получить по URL самостоятельно по следующему шаблону: `<https://storage.yandexcloud.net/s3-sprint3/cohort_{{> your_cohort_number }}/{{ your_nickname }}/project/{{ report_id }}/{{ file_name }}`

### 3. Получение данных за те даты, которые не вошли в основной отчёт

Метод `GET /get_increment` с датой в формате `2020-01-22T00:00:00`:

```text
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_increment?report_id=TWpBeU5DMHdNeTB3TTFReE16b3lNVG96TlFsaVlYSmhibTky=&date=2024-02-25T00:00:00' \
--header 'X-Nickname: baranov' \
--header 'X-Cohort: 23' \
--header 'X-Project: True' \
--header 'X-API-KEY: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
```

- Если инкремент не сформируется, то вернётся `NOT FOUND` с описанием причины.
- Если инкремент сформирован, то метод вернёт статус `SUCCESS`, `increment_id` и ссылки на скачивание файлов из параметра `s3_path`:
  - [customer_research_inc.csv](src/increment/customer_research_inc.csv)
  - [price_log_inc.csv](src/increment/price_log_inc.csv)
  - [user_order_log_inc.csv](src/increment/user_order_log_inc.csv)
  - [user_activity_log_inc.csv](src/increment/user_activity_log_inc.csv)

```json
{
    "report_id": "TWpBeU5DMHdNeTB3TTFReE16b3lNVG96TlFsaVlYSmhibTky=",
    "date": "2024-02-25 00:00:00",
    "status": "SUCCESS",
    "data": {
        "increment_id": "MjAyNC0wMi0yNVQwMDowMDowMAlUV3BCZVU1RE1IZE5lVEIzVFRGUmVFMTZiM2xOVkc5NlRsRnNhVmxZU21oaWJUa3k9",
        "s3_path": {
            "customer_research_inc": "https://storage.yandexcloud.net/s3-sprint3/cohort_23/baranov/project/MjAyNC0wMi0yNVQwMDowMDowMAlUV3BCZVU1RE1IZE5lVEIzVFRGUmVFMTZiM2xOVkc5NlRsRnNhVmxZU21oaWJUa3k9/customer_research_inc.csv",
            "user_order_log_inc": "https://storage.yandexcloud.net/s3-sprint3/cohort_23/baranov/project/MjAyNC0wMi0yNVQwMDowMDowMAlUV3BCZVU1RE1IZE5lVEIzVFRGUmVFMTZiM2xOVkc5NlRsRnNhVmxZU21oaWJUa3k9/user_order_log_inc.csv",
            "user_activity_log_inc": "https://storage.yandexcloud.net/s3-sprint3/cohort_23/baranov/project/MjAyNC0wMi0yNVQwMDowMDowMAlUV3BCZVU1RE1IZE5lVEIzVFRGUmVFMTZiM2xOVkc5NlRsRnNhVmxZU21oaWJUa3k9/user_activity_log_inc.csv",
            "price_log_inc": "https://storage.yandexcloud.net/s3-sprint3/cohort_23/baranov/project/TWpBeU5DMHdNeTB3TTFReE16b3lNVG96TlFsaVlYSmhibTky=/price_log_inc.csv"
        }
    }
}
```

Файлы также можно получить по URL самостоятельно по следующему шаблону: `<https://storage.yandexcloud.net/s3-sprint3/cohort_{{> your_cohort_number }}/{{ your_nickname }}/project/{{ increment_id }}/{{ file_name }}`

## Этап 1

### Описание этапа

- На данных из витрины mart.f_sales BI-аналитики построили графики total revenue для различных срезов.
- Команда разработки добавила функционал отмены заказов и возврата средств (refunded).
- Новые инкременты с информацией о продажах приходят по API и содержат статус заказа (shipped/refunded).

### Задача

Обновить процессы в пайплайне для дальнейшего развития системы магазина:

- Учесть в витрине `mart.f_sales` статусы `shipped` и `refunded`. Все данные в витрине следует считать `shipped`.
- Обновиить пайплайн с учётом статусов и backward compatibility.

### Реализация

- Необходимо провести миграцию схемы и данных в таблице `mart.f_sales`.
- Таблица фактов `mart.f_sales` должна приобрести вид транзакционной таблицы фактов. Будьте внимательны со статусом `refunded`.
- Чтобы total revenue правильно рассчитывался, строки с `refunded` добавляйте со знаком -.
- Первый инкремент приходит со старым форматом данных — без статуса заказа. Проверьте, что ваш код правильно проставляет статус в этом случае.

## Этап 2

### Описание этапа

### Задача

На основе пайплайна из сквозной задачи спринта наполните витрину mart.f_customer_retention данными по «возвращаемости клиентов» в разрезе недель.

Ваши коллеги решили лучше изучить поведение клиентов. Для этого они хотят исследовать возвращаемость клиентов, или customer retention.
Выяснилось, что на текущий момент отчёт по customer retention строится очень долго. Коллеги попросили вас вычислить нужные метрики в дополнительной витрине.

Эта витрина должна отражать следующую информацию:

- Рассматриваемый период — weekly.
- Возвращаемость клиентов:
  - new — кол-во клиентов, которые оформили один заказ за рассматриваемый период;
  - returning — кол-во клиентов, которые оформили более одного заказа за рассматриваемый период;
  - refunded — кол-во клиентов, которые вернули заказ за рассматриваемый период.
- Доход (revenue) и refunded для каждой категории покупателей.
Благодаря витрине можно будет выяснить, какие категории товаров лучше всего удерживают клиентов.

Схема витрины `mart.f_customer_retention`:

- `new_customers_count` — кол-во новых клиентов (тех, которые сделали только один
заказ за рассматриваемый промежуток времени).
- `returning_customers_count` — кол-во вернувшихся клиентов (тех,
которые сделали только несколько заказов за рассматриваемый промежуток времени).
- `refunded_customer_count` — кол-во клиентов, оформивших возврат за
рассматриваемый промежуток времени.
- `period_name` — weekly.
- `period_id` — идентификатор периода (номер недели или номер месяца).
- `item_id` — идентификатор категории товара.
- `new_customers_revenue` — доход с новых клиентов.
- `returning_customers_revenue` — доход с вернувшихся клиентов.
- `customers_refunded` — количество возвратов клиентов.

### Реализация

1) Создание витрины `mart.f_customer_retention` для анализа возвращаемости клиентов.
2) Удаление устаревших или уже обработанных записей из витрины `mart.f_customer_retention`.

## Этап 3 (опциональный)

### Описание этапа

- В понедельник обнаружен баг: в бэкенде данные из источника за воскресенье пришли неполные.
- При этом в системе мониторинга не найдено никаких неполадок.
- Баг починили, данные инкремента восстановили.

### Задача

- Нужно чтобы данные в аналитической системе отражали реальную картину.

Перезапустить пайплайн и убедиться, что после перезапуска не появилось дубликатов в витринах mart.f_sales и mart.f_customer_retention.
Источники данных

Получить новый инкремент можно по API, используя метод GET /get_increment.

## Техническая информация для выполнения проекта

1. Привязать личный GitHub-аккаунт к профилю на платформе Яндекс.Практикума.
2. В GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-3`.
3. Скопировать репозиторий на локальный компьютер.
4. Запустить контейнер Docker через команду: ```docker run -d -p 3000:3000 -p 15432:5432 --name=de-project-sprint-3-server cr.yandex/crp1r8pht0n0gl25aug1/project-sprint-3:latest```
5. После запуска контейнера будут доступны:
   - Visual Studio Code
   - Airflow
   - Database
6. Выполнить проект в локальном репозитории.
7. Обновить репозиторий в GutHub-аккаунте.
