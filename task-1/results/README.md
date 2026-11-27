# Задание 1: пакетная обработка для маркетинга

## Выбор решения

Выбран Apache Airflow. Пайплайн тут собирается из многих источников (CSV, PostgreSQL, Kafka), его будут часто менять и дополнять, нужны условия, повторы и уведомления. Airflow описывает пайплайн кодом (DAG на Python), а под каждую внешнюю систему у него есть готовый провайдер. Сам Airflow данные не перемалывает, он управляет шагами.

### Интеграции

Для BigQuery, Redshift, Kafka и Spark есть официальные провайдеры. Внешние API - через провайдер http. Свой код остается только под бизнес-логику.

### Ветвление, условия, триггеры

- Ветвление - `BranchPythonOperator`, условия запуска шагов - trigger rules (например, шаг выполняется, только если предыдущий упал).
- Кроме расписания DAG можно запускать по событию: сенсор ждет файл в бакете или сообщение в Kafka, или DAG стартует, когда другой DAG обновил данные (Datasets). Плюс ручной запуск и REST API.

### Fallback, retry, email

- Retry настраивается на каждом шаге: число повторов, задержка, экспоненциальный рост задержки.
- Fallback - отдельные шаги с `trigger_rule="one_failed"` и колбэк `on_failure_callback`.
- Email - `EmailOperator` или `send_email` в колбэках, нужен только SMTP.

В UI видно статусы и логи каждого шага, упавший шаг можно перезапустить отдельно.

### Развертывание в облаке

В Google Cloud Platform это Cloud Composer, в AWS - MWAA, либо свой Kubernetes с официальным Helm-чартом. DAG при этом не меняются.

## POC

### Состав

- `airflow` - Airflow в режиме standalone (веб-интерфейс и планировщик в одном контейнере, SQLite).
- `orders-db` - заказы и витрина `delivery_mart`.
- `mailpit` - локальный SMTP с веб-интерфейсом.

### Пайплайн

DAG [marketing_delivery_batch.py](dags/marketing_delivery_batch.py). Пакет - данные за один день (дата запуска): файл `data/inbox/<дата>.csv` со статусами доставок и заказы за этот день из `orders-db`.

1. `read_orders`, `read_deliveries` - читаем заказы из БД и статусы из CSV.
2. `check_quality` - сверка: строка грязная, если такого заказа нет в БД за этот день или статус неизвестен. Грязных строк больше 10% - идем в `quarantine`, иначе в `load_mart`.
3. `load_mart` - чистые строки пишем в витрину.
4. `quarantine` - переносим файл в `data/quarantine`, витрину не трогаем.

Обе ветки (чистый день или карантин) являются успешным исходом. Ретраи есть на всех шагах: 3 раза, первая задержка 15 с, дальше растет экспоненциально. Письма отправляют колбэки DAG: при успехе - с выбранной веткой, при ошибке - с id запуска.

### Запуск

```
docker compose up -d
docker compose exec airflow airflow dags unpause marketing_delivery_batch
docker compose exec airflow airflow dags trigger marketing_delivery_batch -e 2026-11-23
```

Airflow - http://localhost:8088 (admin / admin), Mailpit - http://localhost:8025.

Скриншоты в [screenshots](screenshots): успешный запуск с витриной, успешный запуск с карантином, сбой при остановленной orders-db (граф и логи 4 попыток), письма в Mailpit.
