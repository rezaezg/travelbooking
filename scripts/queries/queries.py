
from pyspark.sql.functions import col, date_add, countDistinct, datediff, count
from pyspark.sql.types import DateType
import os

# database configs
host = os.environ.get('POSTGRES_HOST')
port = os.environ.get('POSTGRES_PORT')
user = os.environ.get('POSTGRES_USER')
password = os.environ.get('POSTGRES_PASSWORD')
DBname = os.environ.get('POSTGRES_DB')

def reports(spark):
    # query for calculating monthly count of unique Users (headcount)\
    # who have made a booking for the last 6 months
    headCount(spark)
    # query for users need more than 30 days to make their first booking
    # and from which company are those users 
    firstBooking(spark)
    # query for 7 day rolling total booking amount for March 2023
    sevenDayRolling(spark)


def headCount(spark):
    # reading fact_booking from postgres    
    fact_booking=spark.read.format("jdbc").option("url",f"jdbc:postgresql://{host}/{DBname}") \
              .option("dbtable", fact_booking).option("user", user) \
              .option("password", password).load()
    # reading dim_date from postgres
    dim_date=spark.read.format("jdbc").option("url",f"jdbc:postgresql://{host}/{DBname}") \
              .option("dbtable", dim_date).option("user", user) \
              .option("password", password).load()
    # Load fact_booking table
    fact_booking = spark.read.format("csv").option("header", "true").load("data/booking.csv")
    # Load dim_date table
    dim_date = spark.read.format("csv").option("header", "true").load("data/dim_date.csv")
    # Convert the "date" column to DateType
    dim_date = dim_date.withColumn("date", col("date").cast(DateType()))
    # Calculate the starting date (6 months ago from '2023-05-31')
    starting_date = '2023-05-31'
    starting_date_minus_6_months = date_add(starting_date, -6 * 30)
    # Perform the join and aggregation
    result = fact_booking.join(dim_date, fact_booking["created_date"] == dim_date["date"]) \
        .where(dim_date["date"] >= starting_date_minus_6_months) \
        .groupBy(dim_date["date"]) \
        .agg(countDistinct(fact_booking["user_id"]).alias("unique_users")) \
        .orderBy(dim_date["date"])
    # Show the result
    result.show()


def firstBooking(spark):
    # reading fact_booking from postgres
    fact_booking = spark.read.format("jdbc").option("url",f"jdbc:postgresql://{host}/{DBname}") \
              .option("dbtable", fact_booking).option("user", user) \
              .option("password", password).load()
    # reading dim_user from postgres
    dim_user = spark.read.format("jdbc").option("url",f"jdbc:postgresql://{host}/{DBname}") \
              .option("dbtable", dim_user).option("user", user) \
              .option("password", password).load()
    # reading dim_company from postgres
    dim_company = spark.read.format("jdbc").option("url",f"jdbc:postgresql://{host}/{DBname}") \
              .option("dbtable", dim_company).option("user", user) \
              .option("password", password).load()
    # Load fact_booking table
    fact_booking = spark.read.format("csv").option("header", "true").load("data/booking.csv")
    # Load dim_user table
    dim_user = spark.read.format("csv").option("header", "true").load("data/users.csv")
    # Load dim_company table
    dim_company = spark.read.format("csv").option("header", "true").load("data/company.csv")
    # Convert date columns to DateType
    fact_booking = fact_booking.withColumn("created_date", col("created_date").cast(DateType()))
    dim_user = dim_user.withColumn("created", col("created").cast(DateType()))
    # calculate the first_booking_date for each user and company
    subquery = fact_booking.groupBy("user_id", "company_id") \
        .agg(min("created_date").alias("first_booking_date"))
    # join and filter
    result = subquery.join(dim_user, subquery["user_id"] == dim_user["userID"]) \
        .join(dim_company, subquery["company_id"] == dim_company["company_id"]) \
        .where(datediff(subquery["first_booking_date"], dim_user["created"]) > 30) \
        .select(subquery["user_id"], subquery["company_id"], dim_company["company_name"])
    # Show the result
    result.show()


def sevenDayRolling(spark):
    # reading fact_booking from postgres
    fact_booking=spark.read.format("jdbc").option("url",f"jdbc:postgresql://{host}/{DBname}") \
              .option("dbtable", fact_booking).option("user", user) \
              .option("password", password).load()
    # reading dim_date from postgres
    dim_date=spark.read.format("jdbc").option("url",f"jdbc:postgresql://{host}/{DBname}") \
              .option("dbtable", dim_date).option("user", user) \
              .option("password", password).load()
    # Load dim_date table
    dim_date = spark.read.format("csv").option("header", "true").load("data/dim_date.csv")
    # Convert date column to DateType
    dim_date = dim_date.withColumn("date", col("date").cast(DateType()))
    # start and end dates
    start_date = '2023-03-01'
    end_date = '2023-03-31'
    # filter a date
    d1 = dim_date.filter((dim_date["date"] >= start_date) & (dim_date["date"] <= end_date))
    d2 = dim_date.withColumn("date", date_add(d1["date"], 6))
    # join two df
    result = d1.join(d2, d1["date"] == d2["date"] - 6, "inner") \
        .join(fact_booking, (fact_booking["created_date"] >= d1["date"]) & (fact_booking["created_date"] <= d2["date"]), "left") \
        .groupBy(d1["date"].alias("start_date"), d2["date"].alias("end_date")) \
        .agg(count(fact_booking["booking_id"]).alias("booking_count")) \
        .orderBy(d1["date"])
    result.show()




