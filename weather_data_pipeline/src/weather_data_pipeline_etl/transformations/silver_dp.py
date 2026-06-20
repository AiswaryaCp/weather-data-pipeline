from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_unixtime, to_timestamp

VALID_CONDITION = (
    (col("main.humidity").between(0, 100))
    & (col("clouds.all").between(0, 100))
    & (col("main.temp").between(-50, 60))
    & (col("main.pressure") > 0)
    & (col("wind.speed") >= 0)
    & (col("visibility") >= 0)
    & (col("name").isNotNull())
    & (col("sys.country").isNotNull())
    & (col("weather_timestamp").isNotNull())
)

@dp.table(
    name="silver_weather_quarantine",
    table_properties={"quality": "silver"},
    comment="Records that failed weather data quality validation rules"
)
def silver_weather_quarantine():
    return (
        spark.readStream.table("bronze_weather")\
        .filter(~VALID_CONDITION)
    )

@dp.table(
    name="silver_weather",
    table_properties={"quality": "silver"},
    comment="Cleaned weather observations enriched with weather metrics, location details, and observation timestamps"
)
@dp.expect(
    "city_not_null",
    "city IS NOT NULL"
)
@dp.expect(
    "country_not_null",
    "country IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_humidity",
    "humidity BETWEEN 0 AND 100"
)
@dp.expect_or_drop(
    "valid_cloud_percentage",
    "cloud_percentage BETWEEN 0 AND 100"
)
@dp.expect_or_drop(
    "valid_temperature",
    "temperature BETWEEN -50 AND 60"
)
@dp.expect(
    "valid_pressure",
    "pressure > 0"
)
@dp.expect(
    "valid_wind_speed",
    "wind_speed >= 0"
)
@dp.expect(
    "valid_visibility",
    "visibility >= 0"
)
@dp.expect(
    "valid_weather_timestamp",
    "weather_timestamp IS NOT NULL"
)
@dp.expect(
    "valid_ingestion_timestamp",
    "ingestion_timestamp IS NOT NULL"
)
def silver_weather():
    df = spark.readStream.table("bronze_weather")\
        .filter(VALID_CONDITION)
    df = df.select(
        col("name").alias("city"),
        col("id").alias("city_id"),
        col("coord.lon").alias("longitude"),
        col("coord.lat").alias("latitude"),
        col("sys.country").alias("country"),
    
        col("main.temp").alias("temperature"),
        col("main.feels_like").alias("feels_like"),
        col("main.humidity").alias("humidity"),
        col("main.pressure").alias("pressure"),
        col("main.sea_level").alias("sea_level"),
        col("main.grnd_level").alias("ground_level"),

        col("visibility"),

        col("wind.speed").alias("wind_speed"),
        col("wind.deg").alias("wind_degree"),
        col("wind.gust").alias("wind_gust"),

        col("clouds.all").alias("cloud_percentage"),

        col("weather")[0]["id"].alias("weather_condition_id"),
        col("weather")[0]["main"].alias("weather_condition"),
        col("weather")[0]["description"].alias("weather_description"),

        col("timezone").alias("timezone_offset"),

        from_unixtime(col("sys.sunrise"))
            .cast("timestamp")
            .alias("sunrise_time"),

        from_unixtime(col("sys.sunset"))
            .cast("timestamp")
            .alias("sunset_time"),

        from_unixtime(col("weather_timestamp"))
            .cast("timestamp")
            .alias("weather_timestamp"),

        to_timestamp(col("ingestion_timestamp"))\
        .alias("ingestion_timestamp"),

        col("source"),
        col("city_requested")
    )

    df = df.dropDuplicates(["city_id", "weather_timestamp"])
    return df