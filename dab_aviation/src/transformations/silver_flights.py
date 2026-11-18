from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view
@dp.expect("valid_distance", "distance_km > 0")
@dp.expect("positive_delay", "coalesce(departure_delay_min, 1) >= 0")
@dp.expect("known_fleet", "is_aircraft_known = 1")
@dp.expect("valid_route", "is_valid_route = 1")
@dp.expect("valid_times", "scheduled_arrival > scheduled_departure")
def silver_flights():
    df_flights = (
        spark.read.table("aviation.dev.bronze_flights")
        .select(
            F.expr("try_cast(flight_id AS BIGINT)").alias("flight_id"),
            "flight_number",
            "tail_number",
            "origin",
            "destination",
            F.expr("try_cast(scheduled_departure AS TIMESTAMP)").alias("scheduled_departure"),
            F.expr("try_cast(scheduled_arrival AS TIMESTAMP)").alias("scheduled_arrival"),
            F.expr("try_cast(distance_km AS DOUBLE)").alias("distance_km"),
            "status",
        )
        .dropDuplicates(["flight_id"])
    )

    df_delays = (
        spark.read.table("aviation.dev.bronze_flight_delays")
        .select(
            F.expr("try_cast(flight_id AS BIGINT)").alias("flight_id"),
            F.expr("try_cast(departure_delay_min AS INT)").alias("departure_delay_min"),
        )
        .dropDuplicates(["flight_id"])
    )

    df_aircrafts = (
        spark.read.table("aviation.dev.bronze_aircraft")
        .select(
            F.expr("try_cast(tail_number AS STRING)").alias("tail_number")
        )
    ).dropDuplicates(["tail_number"])

    df_airports = (spark.read.table("aviation.dev.bronze_airports")).select("airport_code").dropDuplicates(["airport_code"])

    df = (
        df_flights.alias("fl")
        .join(df_delays.alias("fd"), F.col("fl.flight_id") == F.col("fd.flight_id"), "left")
        .join(df_aircrafts.alias("a"), F.col("fl.tail_number") == F.col("a.tail_number"), how="left")
        .join(df_airports.alias("o"), F.col("fl.origin") == F.col("o.airport_code"), how="left")
        .join(df_airports.alias("d"), F.col("fl.destination") == F.col("d.airport_code"), how="left")
        .select(
            "fl.flight_id",
            "fl.flight_number",
            "fl.tail_number",
            "fl.origin",
            "fl.destination",
            "fl.scheduled_departure",
            "fl.scheduled_arrival",
            "fl.distance_km",
            "fl.status",
            F.expr(
                "timestampdiff(MINUTE, fl.scheduled_departure, fl.scheduled_arrival)"
            ).alias("duration_in_minutes"),
            "fd.departure_delay_min",
            F.when(F.col("fd.departure_delay_min").isNull(), "unknown")
             .when(F.col("fd.departure_delay_min") < 15, "on-time")
             .when(F.col("fd.departure_delay_min") <= 30, "15-30min")
             .otherwise("30+min")
             .alias("delay_bucket"),
            F.concat(F.col("fl.origin"), F.lit("-"), F.col("fl.destination")).alias("route"),
            F.expr("CASE WHEN a.tail_number IS NOT NULL THEN 1 ELSE 0 END").alias("is_aircraft_known"),
            F.expr("CASE WHEN o.airport_code IS NOT NULL AND d.airport_code IS NOT NULL THEN 1 ELSE 0 END").alias("is_valid_route")
        )
        .where(
            F.col("flight_id").isNotNull()
        )
    )


    return df