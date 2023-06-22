from pyspark.sql.types import StructType, StructField, StringType, \
                              IntegerType, TimestampType


def cleaning(spark, df_booking, df_company, df_users):
    # clean booking df
    df_booking = clean_booking(spark, df_booking)
    # clean company df
    df_company = clean_company(spark, df_company)
    # clean users df
    df_users = clean_users(spark, df_users)
    return df_booking, df_company, df_users


def clean_booking(spark, df_booking):
    # defining schema
    schema = StructType([StructField("user_id", IntegerType(), True),
                        StructField("booking_id", IntegerType(), True),
                        StructField("created_at", TimestampType(), True),
                        StructField("status", StringType(), True),
                        StructField("checkin_status", StringType(), True),
                        StructField("booking_start_time", TimestampType(), True),
                        StructField("booking_end_time", TimestampType(), True),
                        StructField("is_demo", StringType(), True)])
    df_booking = spark.read.format("csv").option("header", "true") \
                      .schema(schema).load("./data/booking.csv")
    # clean null data of booking df according user id and booking id
    df_booking = df_booking.na.drop(subset=['user_id', 'booking_id'])
    # drop duplicate data from booking according user id and booking id
    df_booking = df_booking.dropDuplicates(['user_id', 'booking_id'])
    return df_booking


def clean_company(spark, df_company):
    schema = StructType([StructField("company_id", IntegerType(), True),
                         StructField("status", StringType(), True),
                         StructField("created_at", TimestampType(), True),
                         StructField("company_name", StringType(), True)])
    df_company = spark.read.format("csv").option("header", "true") \
                      .schema(schema).load("./data/company.csv")
    # clean null data of companys df according company id  column
    df_company = df_company.na.drop(subset=['company_id'])
    # drop duplicate data from companys according company_id and company_name
    df_company = df_company.dropDuplicates(['company_id', 'company_name'])
    return df_company


def clean_users(spark, df_users):
    schema = StructType([StructField("rn", IntegerType(), True),
                         StructField("created_at", TimestampType(), True),
                         StructField("company_id", IntegerType(), True),
                         StructField("status", StringType(), True),
                         StructField("demo_user", StringType(), True)])
    df_users = spark.read.format("csv").option("header", "true") \
                    .schema(schema).load("./data/users.csv")

    # clean null data of users according rn column
    df_users = df_users.na.drop(subset=['rn'])
    # drop duplicate data from users according rn
    df_users = df_users.dropDuplicates(['rn'])
    return df_users
