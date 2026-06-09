import os
import sys

# Wymagane na Windows: Spark potrzebuje HADOOP_HOME z winutils.exe
_hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
_hadoop_bin = os.path.join(_hadoop_home, "bin")
os.makedirs(_hadoop_bin, exist_ok=True)
os.environ.setdefault("HADOOP_HOME", _hadoop_home)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")
os.environ.setdefault("JAVA_TOOL_OPTIONS", f"-Djava.library.path={_hadoop_bin}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, when, floor, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
)

# 1. Konfiguracja połączeń
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "veturilo_db")
DB_USER = os.environ.get("DB_USER", "veturilo_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "veturilo_password")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Rozmiar okna czasowego do bucketu (15 minut = 900 sekund)
# Oba strumienie są mapowane na ten sam bucket jeśli ich timestampy mieszczą się
# w tym samym przedziale 15-minutowym — to zapewnia predykat równościowy
# wymagany przez Spark do joinów stream-stream.
BUCKET_SECONDS = 900

print("-----------------------------------------------------------------")
print(f"Uruchamianie aplikacji PySpark...")
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"PostgreSQL: {JDBC_URL}")
print("-----------------------------------------------------------------")

# 2. Inicjalizacja Sesji Spark
spark = SparkSession.builder \
    .appName("VeturiloRealTimeAnalysis") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Definicja schematów danych z Kafki
veturilo_schema = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("bikes_available", IntegerType(), True),
    StructField("bike_racks", IntegerType(), True),
    StructField("free_racks", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

weather_schema = StructType([
    StructField("temp", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# 4. Odczyt strumieni z Kafki
veturilo_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "veturilo-raw") \
    .option("startingOffsets", "latest") \
    .load()

weather_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "weather-raw") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Przetwarzanie i normalizacja danych wejściowych
# Kolumna v_bucket / w_bucket to klucz równościowy dla joinu stream-stream.
# Spark wymaga predykatu równościowego — sam przedział czasowy nie wystarczy.
veturilo_df = veturilo_raw \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json("json_payload", veturilo_schema).alias("data")) \
    .select(
        col("data.station_id"),
        col("data.name"),
        col("data.bikes_available"),
        col("data.bike_racks"),
        col("data.free_racks"),
        from_unixtime(col("data.timestamp")).cast(TimestampType()).alias("event_time"),
        floor(col("data.timestamp") / BUCKET_SECONDS).cast(LongType()).alias("v_bucket")
    ) \
    .withWatermark("event_time", "10 minutes")

weather_df = weather_raw \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json("json_payload", weather_schema).alias("data")) \
    .select(
        col("data.temp"),
        col("data.rain"),
        from_unixtime(col("data.timestamp")).cast(TimestampType()).alias("weather_time"),
        floor(col("data.timestamp") / BUCKET_SECONDS).cast(LongType()).alias("w_bucket")
    ) \
    .withWatermark("weather_time", "10 minutes")

# 6. Stream-Stream Join z predykatem równościowym (bucket) + okno czasowe
# v_bucket == w_bucket to wymagany predykat równościowy.
# Dodatkowy przedział czasowy ogranicza stan w pamięci (wymagane przy watermarkach).
joined_df = veturilo_df.join(
    weather_df,
    (veturilo_df["v_bucket"] == weather_df["w_bucket"]) &
    expr("event_time >= weather_time - interval 30 minutes") &
    expr("event_time <= weather_time + interval 30 minutes"),
    "inner"
)

# 7. Obliczanie wskaźników KPI
processed_df = joined_df.withColumn(
    "capacity",
    when((col("bikes_available") + col("free_racks")) > 0,
         col("bikes_available") + col("free_racks"))
    .otherwise(when(col("bike_racks") > 0, col("bike_racks")).otherwise(10))
).withColumn(
    "occupancy_rate",
    (col("bikes_available") / col("capacity")) * 100.0
).select(
    col("station_id"),
    col("name"),
    col("bikes_available"),
    col("bike_racks"),
    col("free_racks"),
    col("capacity"),
    col("occupancy_rate"),
    col("temp"),
    col("rain"),
    col("event_time")
)

# 8. Zapis ForeachBatch — PostgreSQL + alerty Kafka
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    print(f"---> Przetwarzanie micro-batcha {batch_id} ({batch_df.count()} rekordów)")

    # A. Zapis danych historycznych do PostgreSQL (bez kolumny capacity)
    try:
        batch_df.drop("capacity") \
            .write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "station_status") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"     [PostgreSQL] Pomyślnie zapisano do station_status.")
    except Exception as e:
        print(f"     [PostgreSQL] Błąd zapisu do station_status: {e}", file=sys.stderr)

    # A1. Statystyki zbiorcze
    try:
        stats = batch_df.agg(
            {"occupancy_rate": "avg", "temp": "avg", "bikes_available": "avg"}
        ).collect()[0]
        print(f"     [Statystyki] Śr. zapełnienie: {stats[0]:.1f}%,"
              f" Śr. temp: {stats[1]:.1f}°C, Śr. rowery: {stats[2]:.1f}")
    except Exception as e:
        print(f"     [Statystyki] Błąd obliczania statystyk: {e}", file=sys.stderr)

    # B. Alerty (< 10% zapełnienia)
    alerts_df = batch_df.filter(col("occupancy_rate") < 10.0)

    if alerts_df.count() > 0:
        alert_count = alerts_df.count()
        print(f"     [Alert] Znaleziono {alert_count} stacji o krytycznym zapełnieniu (< 10%)!")
        for row in alerts_df.select("name", "occupancy_rate").collect():
            print(f"       - {row['name']}: {row['occupancy_rate']:.1f}%")

        # B1. Zapis alertów do PostgreSQL (bez free_racks i capacity)
        try:
            alerts_df.drop("free_racks", "capacity") \
                .write \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", "veturilo_alerts") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"     [PostgreSQL] Pomyślnie zapisano alerty do veturilo_alerts.")
        except Exception as e:
            print(f"     [PostgreSQL] Błąd zapisu alertów do veturilo_alerts: {e}", file=sys.stderr)

        # B2. Publikacja alertów na topic Kafka veturilo-alerts
        try:
            alerts_df.selectExpr(
                "CAST(station_id AS STRING) AS key",
                "to_json(struct(*)) AS value"
            ).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", "veturilo-alerts") \
                .save()
            print(f"     [Kafka] Alerty pomyślnie wysłane do topica veturilo-alerts.")
        except Exception as e:
            print(f"     [Kafka] Błąd wysyłania alertów do topica veturilo-alerts: {e}",
                  file=sys.stderr)
    else:
        print(f"     [Alert] Brak stacji o krytycznym zapełnieniu — system OK.")


# 9. Uruchomienie strumienia
query = processed_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Spark Streaming wystartował pomyślnie. Oczekiwanie na dane ze strumieni...")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nOtrzymano sygnał zatrzymania. Kończenie strumienia...")
    query.stop()
finally:
    spark.stop()
    print("Sesja Spark zakończona.")
