from datetime import datetime
from pyspark.sql.functions import col, date_format, year, month, dayofmonth, floor
from pyspark.sql.types import DateType


def dimdate(spark, start_date, end_date):
    # spark legacy
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    # define dates
    dates = spark.range(0, (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days + 1).selectExpr(f"date_add('{start_date}', cast(id as int)) as date")

    # Extract date fields
    dates = dates.withColumn('year', year(col('date'))) \
                 .withColumn('month', month(col('date'))) \
                 .withColumn('day', dayofmonth(col('date'))) \
                 .withColumn('quarter', floor((col('month') - 1) / 3) + 1) \
                 .withColumn('weekday', date_format(col('date'), 'E')) \
                 .withColumn('day_of_week', date_format(col('date'), 'u').cast('int'))
   
    # Convert date string to date type
    dates = dates.withColumn('date', col('date').cast(DateType()))
    return dates
