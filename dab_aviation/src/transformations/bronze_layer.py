from pyspark import pipelines as dp

base = "/Volumes/aviation/dev/raw"

@dp.table
def bronze_airports():
    return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("pathGlobFilter", f"airports.csv")
    .option("cloudFiles.schemaLocation", f"{base}/_schemas/airports")
    .load(f"{base}/"))
    
@dp.table
def bronze_aircraft():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "aircraft.csv")
        .option("cloudFiles.schemaLocation", f"{base}/_schemas/aircraft")
        .load(base)
    )


@dp.table
def bronze_passengers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "passengers.csv")
        .option("cloudFiles.schemaLocation", f"{base}/_schemas/passengers")
        .load(base)
    )


@dp.table
def bronze_flights():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "flights.csv")
        .option("cloudFiles.schemaLocation", f"{base}/_schemas/flights")
        .load(base)
    )

@dp.table
def bronze_flight_delays():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("pathGlobFilter", "flight_delays.json")
        .option("cloudFiles.schemaHints", "delay_cause VARIANT")
        .option("cloudFiles.schemaLocation", f"{base}/_schemas/flight_delays")
        .load(base)
    )


@dp.table
def bronze_maintenance_log():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "maintenance_log.jsonl")
        .option("cloudFiles.schemaLocation", f"{base}/_schemas/maintenance_log")
        .load(base)
    )


@dp.table
def bronze_bookings():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("pathGlobFilter", "bookings.parquet")
        .option("cloudFiles.schemaLocation", f"{base}/_schemas/bookings")
        .load(base)
    )