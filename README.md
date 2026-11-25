# DWH онлайн-кинотеатра

Из Kafka данные идут в двух направлениях: в ClickHouse для оперативного отчета и в S3 (MinIO), чтобы потом можно было пересчитать данные за прошлые дни.

В ClickHouse используется Kafka Engine. Через materialized view просмотры попадают в таблицу `live_views`. В `live_minute` убираются повторы по `event_id` и считаются количество просмотров и время просмотра для каждого фильма по минутам. Для хранения выбран MergeTree с разбивкой по месяцам, так как данные нужны за разные даты. Просмотры сортируются по ID события и времени, а пакетные витрины — по ID публикации и ключам витрины.

Для архива и ClickHouse используются разные consumer groups (`cinema_archive` и `cinema_clickhouse`), чтобы оба получали все нужные сообщения. В S3 путь к сообщению состоит из топика, partition и offset. Offset подтверждается после сохранения сообщения. Если сообщение прочитается еще раз, оно запишется по тому же пути. Каталог хранится отдельно.

Основной код обработки находится в `scripts/batch.py`. В начале расчета фиксируются offsets Kafka, чтобы работать с одним срезом данных. Данные скачиваются из S3 через boto3, затем читаются в Spark. Промежуточные таблицы сохраняются в Parquet локально и в S3.

В `dags/cinema.py` описан порядок выполнения задач:

```
prepare -> scd2 -> join -> marts -> checks -> publish
```

Сначала проверяются данные и убираются повторы. Затем строится история профилей (SCD2). Если страна и тариф не изменились, новую версию добавлять не нужно. Конец действия версии (`valid_to`) равен началу следующей, а у последней версии он равен NULL. При JOIN с профилями берется версия на момент просмотра (`valid_from <= event_time < valid_to`, у последней версии верхней границы нет). Также просмотры соединяются с каталогом.

В Spark считаются четыре витрины: просмотры по минутам, просмотры по стране и тарифу за день, топ-5 фильмов за день и частоты слов из поисковых запросов. Для последних двух витрин дополнительно сделан расчет через RDD и сравнение с DataFrame. Перед записью проверяется, что нет повторов ключей витрин, интервалы SCD2 не пересекаются и каждому просмотру соответствует ровно один профиль на момент события и один объект каталога. Если проверка не проходит, данные не публикуются.

При записи витрин сохраняются `run_id`, `interval_start`, `interval_end`, `processed_at`. После записи всех четырех витрин добавляется запись в `publications`. При повторном расчете старые данные остаются в таблицах, но в отчет попадает только последняя опубликованная версия за каждый день.

Граница T показывает, до какого момента посчитаны пакетные данные. В `report_all` до T берутся данные из batch, начиная с T — из live. T обновляется после успешной публикации, дни пропускать нельзя. Для простоты расчет идет целыми сутками UTC, Spark работает в режиме `local[2]`, а поздние данные пересчитываются запуском DAG за нужные дни.

## Запуск

Нужны Docker и Docker Compose v2. Все остальное устанавливается в контейнерах. Версии образов и библиотек указаны в `compose.yaml` и `Dockerfile`.

Все команды выполняются из папки `final_project`. Для проверки используется набор `fixture`.

### 1. Запуск контейнеров

Расписание пока отключается, так как будем считать данные за январь 2026 года вручную. Также нужно дать Kafka права на запись в volume.

```bash
echo 'DAG_SCHEDULE=manual' > .env
docker compose run --rm --no-deps --user 0:0 --entrypoint /bin/sh \
  kafka -c 'chown -R 1000:1000 /tmp/kraft-combined-logs'
docker compose up -d --build
docker compose ps
```

Airflow: http://localhost:8080 (admin / admin).

MinIO: http://localhost:9001 (cinema / cinema123). Бакет `cinema`.

### 2. Создание топиков и загрузка каталога

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic cinema.events --partitions 1 --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic cinema.profile_changes --partitions 1 --replication-factor 1

docker compose exec airflow python /opt/airflow/project/load_catalogue.py \
  /data/fixture/catalogue.jsonl
```

Таблицы ClickHouse создаются при первом запуске из `sql/01_schema.sql`.

### 3. Отправка данных и первый расчет

Сначала отправляются данные без поздних сообщений.

```bash
docker compose exec airflow python /opt/airflow/project/replay_kafka.py \
  --dataset /data/fixture --bootstrap-servers kafka:9092 \
  --until-arrival 2026-01-04T00:00:00Z

docker compose exec airflow python /opt/airflow/project/archive_status.py
```

В архиве должно быть 26 событий и 8 изменений профилей. Если их пока меньше, нужно повторить только последнюю команду.

Запуск расчета за 1 января:

```bash
docker compose exec airflow airflow dags unpause cinema
docker compose exec airflow airflow dags trigger cinema \
  --run-id first --conf '{"start":"2026-01-01","end":"2026-01-02"}'
```

Проверка статуса задач:

```bash
docker compose exec airflow airflow tasks states-for-dag-run cinema first
```

После каждого запуска нужно дождаться `success` у всех задач. В команде проверки вместо `first` подставляется название текущего запуска. Результаты после каждого расчета можно посмотреть запросами из следующего раздела.

### 4. Повторный расчет

Новые данные не отправляются. Снова считается 1 января.

```bash
docker compose exec airflow airflow dags trigger cinema \
  --run-id repeat --conf '{"start":"2026-01-01","end":"2026-01-02"}'
```

### 5. Поздние данные

```bash
docker compose exec airflow python /opt/airflow/project/replay_kafka.py \
  --dataset /data/fixture --bootstrap-servers kafka:9092 \
  --from-arrival 2026-01-04T00:00:00Z

docker compose exec airflow python /opt/airflow/project/archive_status.py
```

Когда в архиве будет 28 событий и 9 изменений профилей, снова считаем 1 января:

```bash
docker compose exec airflow airflow dags trigger cinema \
  --run-id late --conf '{"start":"2026-01-01","end":"2026-01-02"}'
```

### 6. Расчет за все три дня

```bash
docker compose exec airflow airflow dags trigger cinema \
  --run-id full --conf '{"start":"2026-01-01","end":"2026-01-04"}'
```

## SQL-запросы для проверки

Вход в ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client \
  --user cinema --password cinema123 --database cinema
```

Оперативная витрина и проверка повторов:

```sql
SELECT count(), uniqExact(event_id) FROM live_views;
SELECT sum(views), sum(watch_time_seconds) FROM live_minute;
```

Граница T и общий отчет:

```sql
SELECT T FROM cutoff;

SELECT toDate(minute) AS day, layer, sum(views) AS views,
       sum(watch_time_seconds) AS seconds
FROM report_all
GROUP BY day, layer
ORDER BY day;
```

Остальные витрины:

```sql
SELECT day, country, subscription_tier, views, watch_time_seconds
FROM report_country ORDER BY day, country;

SELECT day, content_id, views, rank
FROM report_top ORDER BY day, rank;

SELECT day, word, frequency
FROM report_words ORDER BY day, frequency DESC, word;
```

История расчетов и проверка дублей в общем отчете:

```sql
SELECT run_id, interval_start, interval_end, version
FROM publications ORDER BY version;

SELECT DISTINCT run_id, interval_start, interval_end, processed_at
FROM report_country;

SELECT minute, content_id, count()
FROM report_all
GROUP BY minute, content_id
HAVING count() > 1;
```

## Результаты

Все четыре запуска DAG прошли без ошибок. В логах задачи `checks` первого запуска:

```
RDD и DataFrame совпадают: words
RDD и DataFrame совпадают: top
Проверки пройдены
```

### Первый запуск

В Kafka отправлено 34 сообщения, все сохранились в S3. В `live_views` оказалось 20 записей, но уникальных просмотров только 19. После расчета за 1 января получились следующие данные:

| День | Слой | Просмотры | Время, сек. |
|---|---|---:|---:|
| 2026-01-01 | batch | 11 | 1265 |
| 2026-01-02 | live | 6 | 1251 |
| 2026-01-03 | live | 2 | 505 |

T = 2026-01-02 00:00:00. Всего 19 просмотров и 3021 секунда, как и в оперативной витрине. Значит, при объединении слоев просмотры не потерялись и не посчитались два раза.

### Повторный запуск

После `repeat` номер версии изменился с 1 на 2, но в отчете осталось 19 просмотров и 3021 секунда. Граница T и показатели по странам и тарифам не изменились. Запрос на дубли вернул 0 строк.

### Поздние данные

После отправки еще трех сообщений в live стало 20 просмотров и 3141 секунда, но в общем отчете все еще было 19 просмотров и 3021 секунда, так как 1 января уже бралось из batch.

После пересчета за 1 января получилось 12 просмотров вместо 11 и 1385 секунд вместо 1265. T осталась прежней, дублей нет.

Также поменялись данные по странам за 1 января:

| Страна | Тариф | Просмотры до | Просмотры после | Время после, сек. |
|---|---|---:|---:|---:|
| RU | basic | 11 | 8 | 788 |
| KZ | basic | 0 | 4 | 597 |

История профиля пользователя 1001 после пересчета:

| Страна | Тариф | valid_from | valid_to |
|---|---|---|---|
| RU | basic | 2026-01-01 00:00 | 2026-01-01 12:00 |
| KZ | basic | 2026-01-01 12:00 | 2026-01-02 00:00 |
| RU | premium | 2026-01-02 00:00 | 2026-01-03 00:00 |
| RU | basic | 2026-01-03 00:00 | NULL |

Изменение страны пришло позже, но относится к 1 января. Оно добавилось между старыми версиями профиля, поэтому часть просмотров теперь относится к KZ.

### Расчет за весь период

После `full` граница T стала равна 2026-01-04 00:00:00. Теперь все три дня берутся из batch.

| День | Страна | Тариф | Просмотры | Время, сек. |
|---|---|---|---:|---:|
| 2026-01-01 | KZ | basic | 4 | 597 |
| 2026-01-01 | RU | basic | 8 | 788 |
| 2026-01-02 | RU | premium | 6 | 1251 |
| 2026-01-03 | RU | basic | 2 | 505 |

Всего 20 просмотров и 3141 секунда. В топ-5 получилось 12 строк (5 за первый день, 5 за второй и 2 за третий). В поисковых запросах получилось 11 разных слов, каждое встретилось один раз. В SCD2 получилось 7 версий профилей для 3 пользователей. Дублей в общем отчете нет.

## Расписание и остановка

По умолчанию DAG запускается каждый день в 00:00 UTC. Для расчета берется интервал Airflow, а при ручном запуске его можно указать самостоятельно. Для включения расписания:

```bash
rm .env
docker compose up -d airflow
```

При проверке использовался ручной запуск, запуск по расписанию и набор `demo` отдельно не проверялись.

Остановить контейнеры с сохранением данных:

```bash
docker compose down
```

Чтобы начать с нуля, нужно выполнить `docker compose down -v` (все данные будут удалены) и повторить запуск с первого шага. Наборы `fixture` и `demo` не нужно смешивать.
