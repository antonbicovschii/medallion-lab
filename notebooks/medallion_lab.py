# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion lab (Free Edition, serverless)
# MAGIC Источник событий (JSON-файлы) -> Bronze (Auto Loader) -> Silver (dedup, MERGE) -> Gold (почасовые агрегаты).
# MAGIC Триггер везде `availableNow` (единственный, что поддерживает serverless).
# MAGIC Запускай ячейки сверху вниз. Раздел 6 — сценарии.

# COMMAND ----------

# DBTITLE 1,0. Настройки и объекты
import json, uuid, random, time, os
from datetime import datetime, timedelta, timezone
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

CATALOG, SCHEMA = "workspace", "lab"      # в Free Edition каталог по умолчанию обычно `workspace`
LATE_SEC = 3600                           # событие "позднее", если пришло > 1 часа после event_ts

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.chk")

BASE    = f"/Volumes/{CATALOG}/{SCHEMA}"
LANDING = f"{BASE}/landing/events"
STATE   = f"{BASE}/landing/_state"
CHK     = f"{BASE}/chk"
BRONZE, SILVER, GOLD, OPS = [f"{CATALOG}.{SCHEMA}.{t}" for t in
                             ("bronze_events", "silver_events", "gold_hourly", "ops_metrics")]
os.makedirs(LANDING, exist_ok=True); os.makedirs(STATE, exist_ok=True)

spark.sql(f"""CREATE TABLE IF NOT EXISTS {SILVER} (
  event_id STRING, user_id STRING, event_type STRING, amount DOUBLE,
  event_ts TIMESTAMP, _ingest_ts TIMESTAMP, _source_file STRING, is_late BOOLEAN)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD} (
  event_hour TIMESTAMP, event_type STRING, events BIGINT, users BIGINT,
  revenue DOUBLE, late_events BIGINT, _updated_ts TIMESTAMP)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS} (
  run_label STRING, layer STRING, finished_at TIMESTAMP, seconds DOUBLE,
  input_rows BIGINT, micro_batches INT)""")

# COMMAND ----------

# DBTITLE 1,1. Источник событий (имитация Kafka -> файлы)
TYPES = ["view", "click", "add_to_cart", "purchase"]

def generate_batch(n_events=2000, ts_from=None, ts_to=None, dup_rate=0.05,
                   late_rate=0.05, n_files=2):
    """Пишет n_files JSON-lines файлов в LANDING.
    - обычные события: event_ts равномерно в [ts_from, ts_to]
    - late_rate: event_ts на 1-3 дня раньше ts_from (приедут "с опозданием")
    - dup_rate: точные копии ранее отправленных событий (at-least-once доставка)"""
    now = datetime.now(timezone.utc)
    ts_to   = ts_to   or now
    ts_from = ts_from or (now - timedelta(minutes=30))
    span = (ts_to - ts_from).total_seconds()

    state_path = f"{STATE}/emitted.json"
    prev = json.load(open(state_path)) if os.path.exists(state_path) else []

    def new_event(late=False):
        if late:
            ts = ts_from - timedelta(seconds=random.uniform(86400, 3 * 86400))
        else:
            ts = ts_from + timedelta(seconds=random.uniform(0, span))
        return {"event_id": str(uuid.uuid4()), "user_id": f"u{random.randint(1, 500)}",
                "event_type": random.choice(TYPES),
                "amount": round(random.uniform(1, 200), 2) if random.random() < 0.2 else 0.0,
                "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}

    events = [new_event(late=random.random() < late_rate) for _ in range(n_events)]
    n_dup = min(int(n_events * dup_rate), len(prev))
    events += random.sample(prev, n_dup) if n_dup else []
    events += random.sample(events[:n_events], int(n_events * dup_rate / 2))  # дубли внутри батча
    random.shuffle(events)

    stamp = now.strftime("%Y%m%dT%H%M%S%f")
    for i in range(n_files):
        with open(f"{LANDING}/events_{stamp}_{i}.json", "w") as f:
            for e in events[i::n_files]:
                f.write(json.dumps(e) + "\n")

    json.dump((prev + events[:n_events])[-5000:], open(state_path, "w"))
    print(f"записано {len(events)} событий ({n_dup} дублей из прошлого, late≈{late_rate:.0%}) в {n_files} файла")

# COMMAND ----------

# DBTITLE 1,2. Bronze: Auto Loader, сырые данные как есть
def start_bronze():
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", f"{CHK}/bronze_schema")
          .option("cloudFiles.schemaHints", "event_ts TIMESTAMP, amount DOUBLE")
          .load(LANDING)                       # rescued data: неожиданные поля попадут в _rescued_data
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))
    return (df.writeStream.option("checkpointLocation", f"{CHK}/bronze")
            .option("mergeSchema", "true")
            .trigger(availableNow=True).toTable(BRONZE))

# COMMAND ----------

# DBTITLE 1,3. Silver: дедупликация + поздние данные через MERGE по event_id
def upsert_silver(batch_df, batch_id):
    # 1) дубли внутри микробатча: оставляем первую по времени поступления.
    w = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").asc())
    clean = (batch_df.filter("event_id IS NOT NULL AND event_ts IS NOT NULL")
             .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
             .withColumn("is_late", (F.unix_timestamp("_ingest_ts") - F.unix_timestamp("event_ts")) > LATE_SEC)
             .select("event_id", "user_id", "event_type", "amount",
                     "event_ts", "_ingest_ts", "_source_file", "is_late"))
    # 2) дубли с прошлыми батчами: MERGE вставляет только те event_id, которых ещё нет.
    #    Watermark не нужен -> событие с опозданием на 3 дня тоже попадёт, а не потеряется.
    (DeltaTable.forName(spark, SILVER).alias("t")
     .merge(clean.alias("s"), "t.event_id = s.event_id")
     .whenNotMatchedInsertAll().execute())

def start_silver():
    return (spark.readStream.table(BRONZE)
            .writeStream.foreachBatch(upsert_silver)
            .option("checkpointLocation", f"{CHK}/silver")
            .trigger(availableNow=True).start())

# COMMAND ----------

# DBTITLE 1,4. Gold: пересчёт только затронутых часов
def upsert_gold(batch_df, batch_id):
    # новые строки silver (в т.ч. поздние) затрагивают старые часы -> пересчитываем эти часы целиком
    hours = batch_df.select(F.date_trunc("hour", "event_ts").alias("event_hour")).distinct()
    agg = (spark.table(SILVER).withColumn("event_hour", F.date_trunc("hour", "event_ts"))
           .join(F.broadcast(hours), "event_hour", "left_semi")
           .groupBy("event_hour", "event_type")
           .agg(F.count("*").alias("events"), F.countDistinct("user_id").alias("users"),
                F.round(F.sum("amount"), 2).alias("revenue"),
                F.sum(F.col("is_late").cast("int")).alias("late_events"))
           .withColumn("_updated_ts", F.current_timestamp()))
    (DeltaTable.forName(spark, GOLD).alias("t")
     .merge(agg.alias("s"), "t.event_hour = s.event_hour AND t.event_type = s.event_type")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def start_gold():
    return (spark.readStream.table(SILVER)    # silver пополняется только вставками -> безопасный источник стрима
            .writeStream.foreachBatch(upsert_gold)
            .option("checkpointLocation", f"{CHK}/gold")
            .trigger(availableNow=True).start())

# COMMAND ----------

# DBTITLE 1,5. Запуск слоёв + замер (время, строки, батчи)
def run_layer(layer, start_fn, run_label):
    t0 = time.time()
    q = start_fn(); q.awaitTermination()
    secs = round(time.time() - t0, 1)
    rows = sum(p["numInputRows"] for p in q.recentProgress)
    spark.createDataFrame([(run_label, layer, datetime.now(timezone.utc), secs, rows, len(q.recentProgress))],
                          "run_label string, layer string, finished_at timestamp, seconds double, input_rows long, micro_batches int"
                          ).write.mode("append").saveAsTable(OPS)
    print(f"{run_label:<12} {layer:<7} {secs:>6}s  строк на входе: {rows}")

def run_pipeline(run_label):
    run_layer("bronze", start_bronze, run_label)
    run_layer("silver", start_silver, run_label)
    run_layer("gold",   start_gold,   run_label)

def replay_from_bronze(run_label="replay"):
    """Бэкфилл-2: пересобрать silver и gold из bronze (например, после смены логики)."""
    dbutils.fs.rm(f"{CHK}/silver", True); dbutils.fs.rm(f"{CHK}/gold", True)
    spark.sql(f"TRUNCATE TABLE {SILVER}"); spark.sql(f"TRUNCATE TABLE {GOLD}")
    run_layer("silver", start_silver, run_label)
    run_layer("gold",   start_gold,   run_label)

# COMMAND ----------

# DBTITLE 1,6a. Сценарий: обычный батч
generate_batch(n_events=2000)
run_pipeline("live-1")

# COMMAND ----------

# DBTITLE 1,6b. Сценарий: дубли и поздние данные (больше, чем обычно)
generate_batch(n_events=2000, dup_rate=0.15, late_rate=0.15)
run_pipeline("live-2")

# COMMAND ----------

# DBTITLE 1,6c. Бэкфилл: история за 7 дней
now = datetime.now(timezone.utc)
generate_batch(n_events=20000, ts_from=now - timedelta(days=8), ts_to=now - timedelta(days=1),
               dup_rate=0.02, late_rate=0.0, n_files=8)
run_pipeline("backfill")      # Auto Loader видит новые файлы, MERGE идемпотентен -> можно повторять безопасно

# COMMAND ----------

# DBTITLE 1,7. Проверки корректности
display(spark.sql(f"""
SELECT (SELECT count(*) FROM {BRONZE})                                  AS bronze_rows,
       (SELECT count(*) FROM {SILVER})                                  AS silver_rows,
       (SELECT count(DISTINCT event_id) FROM {SILVER})                  AS silver_distinct_ids,
       (SELECT count(*) FROM {BRONZE}) - (SELECT count(*) FROM {SILVER}) AS removed_dups_or_bad,
       (SELECT count(*) FROM {SILVER} WHERE is_late)                    AS late_rows,
       (SELECT sum(events) FROM {GOLD})                                 AS gold_events_total"""))
# Ожидаем: silver_rows = silver_distinct_ids = gold_events_total, removed_dups_or_bad > 0, late_rows > 0

# COMMAND ----------

# DBTITLE 1,8. Отчёт по "стоимости"
display(spark.sql(f"""
SELECT run_label, layer, seconds, input_rows, micro_batches,
       round(input_rows / nullif(seconds, 0)) AS rows_per_sec
FROM {OPS} ORDER BY finished_at"""))

# Размер данных и число файлов (влияет на стоимость чтения/хранения)
for t in (BRONZE, SILVER, GOLD):
    d = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles", "sizeInBytes").first()
    print(f"{t}: files={d.numFiles}, size={d.sizeInBytes/1024:.0f} KB")

# На платном workspace — реальные DBU (в Free Edition таблица может быть недоступна):
try:
    display(spark.sql("""
      SELECT usage_date, sku_name, sum(usage_quantity) AS dbu
      FROM system.billing.usage
      WHERE usage_date >= current_date() - 7 GROUP BY 1, 2 ORDER BY 1"""))
except Exception as e:
    print("system.billing.usage недоступна:", str(e)[:120])
