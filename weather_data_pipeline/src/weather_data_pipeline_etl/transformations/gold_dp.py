from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, to_date, avg, max, min, round


@dp.materialized_view(
    name="gold_latest_weather",
    comment="Latest weather observation for each city"
)
def gold_latest_weather():
    df = spark.read.table("silver_weather")
    window_spec = Window.partitionBy("city_id").orderBy(col("weather_timestamp").desc())
    df = df.withColumn("rn", row_number().over(window_spec))
    df = df.filter(col("rn")==1).drop("rn")
    return df

@dp.materialized_view(
    name="gold_daily_weather_summary",
    comment="Daily weather metrics aggregated by city"
)
def gold_daily_weather_summary():
    df = spark.read.table("silver_weather")
    df = df.withColumn("weather_date", to_date(col("weather_timestamp")))
    df = df.groupBy("city", "weather_date").agg(
        round(avg(col("temperature"))).alias("avg_temperature"),
        max(col("temperature")).alias("max_temperature"),
        min(col("temperature")).alias("min_temperature"),
        round(avg(col("humidity"))).alias("avg_humidity"),
        round(avg(col("wind_speed"))).alias("avg_wind_speed")
    ).orderBy(col("city").asc(), col("weather_date").desc())
    return df