import hashlib
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import Consumer, TopicPartition
from pyspark.sql import SparkSession, Window, functions as F
from storage import s3_client, keys, BUCKET

TABLES = {
    "minute": ["minute", "content_id"],
    "country": ["day", "country", "subscription_tier"],
    "top": ["day", "rank"],
    "words": ["day", "word"]
}


def spark_session():
    spark = (SparkSession.builder.master("local[2]")
             .appName("cinema").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ch(query, data=None):
    response = requests.post(
        "http://clickhouse:8123", auth=("cinema", "cinema123"),
        params={"query": query, "date_time_input_format": "best_effort"},
        data=data, timeout=120
    )
    if not response.ok:
        raise RuntimeError(response.text)
    return response.text.strip()


def save(df, folder, name):
    if name in TABLES or name == "scd2":
        snapshot = json.loads((folder / "snapshot.json").read_text())
        df = (df.withColumn("run_id", F.lit(snapshot["run_id"]))
              .withColumn("interval_start", F.lit(snapshot["start"]).cast("timestamp"))
              .withColumn("interval_end", F.lit(snapshot["end"]).cast("timestamp"))
              .withColumn("processed_at", F.current_timestamp()))
    path = folder / name
    df.write.mode("overwrite").parquet(str(path))
    s3 = s3_client()
    prefix = f"runs/{folder.name}/{name}/"
    for key in keys(s3, prefix):
        s3.delete_object(Bucket=BUCKET, Key=key)
    for file in path.glob("part-*.parquet"):
        s3.upload_file(str(file), BUCKET, prefix + file.name)


def read(spark, folder, name):
    return spark.read.parquet(str(folder / name))


def require(condition, text):
    if not condition:
        raise ValueError(text)


def archive_keys(s3):
    consumer = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "cinema_snapshot"})
    result = []
    try:
        for topic in ["cinema.events", "cinema.profile_changes"]:
            partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
            for partition in partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
                prefix = f"raw/{topic}/{partition}/"
                files = keys(s3, prefix)
                offsets = {int(Path(key).stem): key for key in files}
                require(sum(low <= offset < high for offset in offsets) == high - low,
                        "Архив еще не догнал Kafka: " + topic)
                result.extend(key for offset, key in offsets.items() if offset < high)
    finally:
        consumer.close()
    return sorted(result)


def prepare(spark, folder, start, end, run_id):
    s3 = s3_client()
    snapshot = folder / "snapshot.json"
    if not snapshot.exists():
        raw = archive_keys(s3)
        require(bool(raw), "Архив Kafka пуст")
        s3.download_file(BUCKET, "catalogue/catalogue.jsonl", str(folder / "catalogue.jsonl"))
        s3.upload_file(str(folder / "catalogue.jsonl"), BUCKET, f"runs/{folder.name}/catalogue.jsonl")
        temporary = folder / "snapshot.tmp"
        temporary.write_text(json.dumps({"start": start, "end": end, "run_id": run_id, "keys": raw}), encoding="utf-8")
        temporary.replace(snapshot)
    s3.upload_file(str(snapshot), BUCKET, f"runs/{folder.name}/snapshot.json")
    raw = json.loads(snapshot.read_text())["keys"]

    for topic, name in [("cinema.events", "events"), ("cinema.profile_changes", "profiles")]:
        with (folder / (name + ".jsonl")).open("wb") as output:
            for key in raw:
                if key.startswith("raw/" + topic + "/"):
                    row = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                    output.write(row.rstrip(b"\n") + b"\n")

    events = spark.read.schema(
        "event_id string, event_time string, user_id long, event_type string, "
        "content_id long, watch_time_seconds long, query string, available_at string"
    ).json(str(folder / "events.jsonl"))
    events = events.withColumn("event_time", F.to_timestamp("event_time"))
    valid = (F.col("event_id").isNotNull() & F.col("user_id").isNotNull() &
             F.col("event_time").isNotNull() & F.col("available_at").isNotNull() &
             (((F.col("event_type") == "view") & F.col("content_id").isNotNull() &
               (F.col("content_id") >= 0) & F.col("watch_time_seconds").isNotNull() &
               (F.col("watch_time_seconds") >= 0) & F.col("query").isNull()) |
              ((F.col("event_type") == "search") & F.col("query").isNotNull() &
               F.col("content_id").isNull() & F.col("watch_time_seconds").isNull())))
    require(events.filter(~F.coalesce(valid, F.lit(False))).count() == 0, "Некорректные события")
    events = events.drop("available_at").dropDuplicates()
    require(events.groupBy("event_id").count().filter("count > 1").count() == 0, "Разные события с одним ID")
    events = events.filter((F.col("event_time") >= F.lit(start).cast("timestamp")) &
                           (F.col("event_time") < F.lit(end).cast("timestamp")))

    profiles = spark.read.schema(
        "change_id string, user_id long, effective_at string, country string, subscription_tier string, available_at string"
    ).json(str(folder / "profiles.jsonl"))
    profiles = profiles.withColumn("effective_at", F.to_timestamp("effective_at"))
    valid = (F.col("change_id").isNotNull() & F.col("user_id").isNotNull() &
             F.col("effective_at").isNotNull() & F.col("available_at").isNotNull() &
             F.col("country").isin("RU", "KZ", "BY", "AM") &
             F.col("subscription_tier").isin("basic", "standard", "premium"))
    require(profiles.filter(~F.coalesce(valid, F.lit(False))).count() == 0, "Некорректные профили")
    profiles = profiles.drop("available_at").dropDuplicates()
    require(profiles.groupBy("change_id").count().filter("count > 1").count() == 0, "Разные изменения с одним ID")

    catalogue = spark.read.schema("content_id long, category string, duration_seconds long").json(str(folder / "catalogue.jsonl"))
    require(catalogue.filter("content_id is null or content_id < 0 or category is null or duration_seconds is null or duration_seconds <= 0").count() == 0, "Некорректный каталог")
    require(catalogue.groupBy("content_id").count().filter("count > 1").count() == 0, "Дубли в каталоге")
    save(events, folder, "events")
    save(profiles, folder, "profiles")
    save(catalogue, folder, "catalogue")
    print("Уникальных событий за период:", events.count())


def scd2(spark, folder):
    profiles = read(spark, folder, "profiles").drop("change_id").dropDuplicates()
    require(profiles.groupBy("user_id", "effective_at").count().filter("count > 1").count() == 0,
            "Разные состояния в один момент")
    window = Window.partitionBy("user_id").orderBy("effective_at")
    profiles = profiles.withColumn("previous_country", F.lag("country").over(window))
    profiles = profiles.withColumn("previous_tier", F.lag("subscription_tier").over(window))
    history = profiles.filter(
        F.col("previous_country").isNull() | (F.col("country") != F.col("previous_country")) |
        (F.col("subscription_tier") != F.col("previous_tier"))
    ).drop("previous_country", "previous_tier")
    history = history.withColumn("valid_to", F.lead("effective_at").over(window))
    history = history.withColumnRenamed("effective_at", "valid_from")
    history = history.withColumn("is_current", F.col("valid_to").isNull())
    save(history, folder, "scd2")


def join_views(spark, folder):
    views = read(spark, folder, "events").filter("event_type = 'view'")
    catalogue = read(spark, folder, "catalogue")
    history = read(spark, folder, "scd2")
    joined = views.join(catalogue, "content_id", "left")
    joined = joined.alias("v").join(history.alias("p"),
        (F.col("v.user_id") == F.col("p.user_id")) &
        (F.col("v.event_time") >= F.col("p.valid_from")) &
        (F.col("p.valid_to").isNull() | (F.col("v.event_time") < F.col("p.valid_to"))), "left")
    joined = joined.select("v.*", "p.country", "p.subscription_tier", "p.valid_from")
    save(joined, folder, "joined")


def marts(spark, folder):
    joined = read(spark, folder, "joined").withColumn("day", F.to_date("event_time"))
    minute = joined.withColumn("minute", F.date_trunc("minute", "event_time"))
    minute = minute.groupBy("minute", "content_id").agg(
        F.count("*").alias("views"), F.sum("watch_time_seconds").alias("watch_time_seconds"))
    country = joined.groupBy("day", "country", "subscription_tier").agg(
        F.count("*").alias("views"), F.sum("watch_time_seconds").alias("watch_time_seconds"))
    top = joined.groupBy("day", "content_id").agg(F.count("*").alias("views"))
    window = Window.partitionBy("day").orderBy(F.desc("views"), "content_id")
    top = top.withColumn("rank", F.row_number().over(window)).filter("rank <= 5")
    searches = read(spark, folder, "events").filter("event_type = 'search'")
    words = searches.withColumn("day", F.to_date("event_time"))
    words = words.withColumn("word", F.explode(F.split(F.lower("query"), r"(?U)\s+")))
    words = words.filter(F.length("word") > 4).groupBy("day", "word").agg(F.count("*").alias("frequency"))
    for name, df in [("minute", minute), ("country", country), ("top", top), ("words", words)]:
        save(df, folder, name)


def compare_rdd(spark, folder):
    events = read(spark, folder, "events").withColumn("day", F.to_date("event_time"))
    searches = events.filter("event_type = 'search'").select("day", "query").rdd
    words = searches.flatMap(lambda row: [((row.day, word), 1) for word in row.query.lower().split() if len(word) > 4])
    words = words.reduceByKey(lambda a, b: a + b).map(lambda row: (row[0][0], row[0][1], row[1]))
    words = spark.createDataFrame(words, "day date, word string, frequency long")
    views = events.filter("event_type = 'view'").select("day", "content_id").rdd
    top = views.map(lambda row: ((row.day, row.content_id), 1)).reduceByKey(lambda a, b: a + b)
    top = top.map(lambda row: (row[0][0], (row[0][1], row[1]))).groupByKey()
    top = top.flatMap(lambda row: [(row[0], item[0], item[1], rank)
                                  for rank, item in enumerate(sorted(row[1], key=lambda item: (-item[1], item[0]))[:5], 1)])
    top = spark.createDataFrame(top, "day date, content_id long, views long, rank int")
    for name, rdd_result in [("words", words), ("top", top)]:
        df_result = read(spark, folder, name).select(*rdd_result.columns)
        require(df_result.exceptAll(rdd_result).count() == 0 and rdd_result.exceptAll(df_result).count() == 0,
                "RDD и DataFrame не совпадают: " + name)
        save(rdd_result, folder, "rdd_" + name)
        print("RDD и DataFrame совпадают:", name)


def checks(spark, folder):
    history = read(spark, folder, "scd2")
    window = Window.partitionBy("user_id").orderBy("valid_from")
    history = history.withColumn("next_from", F.lead("valid_from").over(window))
    require(history.filter(
        (F.col("valid_to").isNotNull() & (F.col("valid_from") >= F.col("valid_to"))) |
        (~F.col("valid_to").eqNullSafe(F.col("next_from")))
    ).count() == 0, "Некорректные интервалы SCD2")
    joined = read(spark, folder, "joined")
    views = read(spark, folder, "events").filter("event_type = 'view'")
    require(joined.filter("category is null or valid_from is null or country is null or subscription_tier is null").count() == 0,
            "Просмотр без каталога или профиля")
    require(joined.groupBy("event_id").count().filter("count != 1").count() == 0 and joined.count() == views.count(),
            "JOIN изменил количество просмотров")
    for name, columns in TABLES.items():
        df = read(spark, folder, name)
        require(df.groupBy(*columns).count().filter("count > 1").count() == 0, "Дубли в витрине " + name)
    compare_rdd(spark, folder)
    print("Проверки пройдены")


def publish(spark, folder, start, end, run_id):
    cutoff = ch("SELECT toString(T) FROM cinema.cutoff")[:10]
    require(start <= cutoff, "Нельзя пропускать дни до T: " + cutoff)
    publication_id = uuid.uuid4().hex
    processed = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    metadata = {"run_id": run_id, "interval_start": start + " 00:00:00",
                "interval_end": end + " 00:00:00", "processed_at": processed,
                "publication_id": publication_id}
    for name in TABLES:
        df = read(spark, folder, name).drop("run_id", "interval_start", "interval_end", "processed_at")
        rows = []
        for row in df.toJSON().toLocalIterator():
            rows.append(json.dumps({**json.loads(row), **metadata}, ensure_ascii=False))
            if len(rows) == 1000:
                ch(f"INSERT INTO cinema.batch_{name} FORMAT JSONEachRow", "\n".join(rows).encode())
                rows = []
        if rows:
            ch(f"INSERT INTO cinema.batch_{name} FORMAT JSONEachRow", "\n".join(rows).encode())
        count = int(ch(f"SELECT count() FROM cinema.batch_{name} WHERE publication_id = '{publication_id}'"))
        require(count == df.count(), "Не все строки записаны: " + name)
    version = int(ch("SELECT max(version) FROM cinema.publications")) + 1
    ch("INSERT INTO cinema.publications FORMAT JSONEachRow", json.dumps({**metadata, "version": version}).encode())
    print("Публикация:", publication_id, "T:", ch("SELECT T FROM cinema.cutoff"))


def run(stage, **context):
    run_id = context["run_id"]
    folder = Path("/opt/airflow/work") / hashlib.sha256(run_id.encode()).hexdigest()[:24]
    folder.mkdir(parents=True, exist_ok=True)
    snapshot = folder / "snapshot.json"
    if snapshot.exists():
        saved = json.loads(snapshot.read_text())
        start, end = saved["start"], saved["end"]
    else:
        config = context["dag_run"].conf or {}
        if config:
            start, end = config["start"], config["end"]
        else:
            cutoff = ch("SELECT toString(T) FROM cinema.cutoff")[:10]
            start = min(cutoff, context["data_interval_start"].date().isoformat())
            end = context["data_interval_end"].date().isoformat()
        require(date.fromisoformat(start).isoformat() == start and date.fromisoformat(end).isoformat() == end,
                "Нужны даты YYYY-MM-DD")
        require("2026-01-01" <= start < end, "Некорректный интервал")
        require(start <= ch("SELECT toString(T) FROM cinema.cutoff")[:10], "Нельзя пропускать дни до T")

    spark = spark_session()
    try:
        if stage == "prepare":
            prepare(spark, folder, start, end, run_id)
        elif stage == "publish":
            publish(spark, folder, start, end, run_id)
        else:
            {"scd2": scd2, "join": join_views, "marts": marts, "checks": checks}[stage](spark, folder)
    finally:
        spark.stop()
