from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view
@dp.expect("valid_base_fare", "base_fare_try > 0")
@dp.expect("valid_total_fare", "total_fare >= 0")
@dp.expect("valid_booking_date", "days_booked_in_advance >= 0")
@dp.expect("known_flight", "flight_id IS NOT NULL")
@dp.expect("known_customer", "customer_id IS NOT NULL")
def silver_bookings():
    df_bookings = (
        spark.read.table("aviation.dev.bronze_bookings")
        .select(
            F.expr("try_cast(booking_id AS BIGINT)").alias("booking_id"),
            F.expr("try_cast(flight_id AS BIGINT)").alias("flight_id"),
            F.expr("try_cast(customer_id AS BIGINT)").alias("customer_id"),
            F.expr("try_cast(booking_date AS DATE)").alias("booking_date"),
            "fare_class",
            F.expr("try_cast(base_fare_try AS DOUBLE)").alias("base_fare_try"),
            F.expr("try_cast(taxes_try AS DOUBLE)").alias("taxes_try"),
            "seat",
            "booking_channel",
            "status",
        )
        .dropDuplicates(["booking_id"])
    )

    df_flights = (
        spark.read.table("aviation.dev.bronze_flights")
        .select(
            F.expr("try_cast(flight_id AS BIGINT)").alias("flight_id"),
            F.expr("try_cast(scheduled_departure AS TIMESTAMP)").alias("scheduled_departure"),
        )
        .dropDuplicates(["flight_id"])
    )

    df_passengers = (
        spark.read.table("aviation.dev.bronze_passengers")
        .select(
            F.expr("try_cast(customer_id AS BIGINT)").alias("customer_id"),
            "loyalty_tier",
        )
        .dropDuplicates(["customer_id"])
    )

    df = (
        df_bookings.alias("b")
        .join(df_flights.alias("f"), F.col("b.flight_id") == F.col("f.flight_id"), "left")
        .join(df_passengers.alias("p"), F.col("b.customer_id") == F.col("p.customer_id"), "left")
        .select(
            "b.booking_id",
            "b.flight_id",
            "b.customer_id",
            "b.booking_date",
            "b.fare_class",
            "b.base_fare_try",
            "b.taxes_try",
            "b.seat",
            "b.booking_channel",
            "b.status",
            (F.col("b.base_fare_try") + F.col("b.taxes_try")).alias("total_fare"),
            F.expr("timestampdiff(DAY, b.booking_date, f.scheduled_departure)")
             .alias("days_booked_in_advance"),
            "p.loyalty_tier",
            F.when(F.col("p.loyalty_tier") == "Blue", 0.1)
             .when(F.col("p.loyalty_tier") == "Silver", 0.2)
             .when(F.col("p.loyalty_tier") == "Gold", 0.3)
             .when(F.col("p.loyalty_tier") == "Platinum", 0.4)
             .otherwise(0.0)
             .alias("loyalty_discount"),
        )
        .where(F.col("booking_id").isNotNull())
    )

    return df