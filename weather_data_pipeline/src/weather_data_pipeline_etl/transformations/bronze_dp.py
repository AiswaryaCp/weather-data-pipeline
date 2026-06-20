from pyspark import pipelines as dp

@dp.table(
    name="bronze_weather",
    table_properties={"quality":"bronze"},
    comment="Raw weather data of different cities"
)
def bronze_weather():
    df = spark.readStream\
        .format("cloudFiles")\
            .option("cloudFiles.format", "json")\
                .option("cloudFiles.inferColumnTypes", "true")\
                    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
                        .load("s3://weather-data-bucket-aiswarya/weather/")
    return df