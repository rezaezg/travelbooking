from pyspark.sql.functions import col
from scripts.Loading.Load_to_db import save_dim_to_db, Load_to_db
import os

# database configs
host = os.environ.get('POSTGRES_HOST')
port = os.environ.get('POSTGRES_PORT')
user = os.environ.get('POSTGRES_USER')
password = os.environ.get('POSTGRES_PASSWORD')
DBname = os.environ.get('POSTGRES_DB')
table_name = "dim_date"

def TestLoadToDB(spark):
    # test writing dataframe
    test_save_dim_to_db(spark)
    # test loading to postgres
    test_load_to_db(spark)

def test_save_dim_to_db(spark):
    # define date
    dim_date_data = [(1, '2022-10-01'), (2, '2022-10-02'), (3, '2022-10-03')]
    # create dataframe
    dim_date = spark.createDataFrame(dim_date_data, ['date_id', 'date'])
    # write dataframe
    save_dim_to_db(dim_date, host, DBname, user, password, table_name)
    # read dim_date from postgres
    saved_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}/{DBname}") \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .load()
    # compare dataframe with data in postgres
    assert saved_df.count() == dim_date.count()


def test_load_to_db(spark):
    # define dim_date
    dim_date_data = [(1, '2021-01-01'), (2, '2021-01-02'), (3, '2021-01-03')]
    dim_date = spark.createDataFrame(dim_date_data, ['date_id', 'date'])
    # define booking dataframe
    df_booking_data = [(1, 'status1', 'checkin1'), (2, 'status2', 'checkin2')]
    df_booking = spark.createDataFrame(df_booking_data, ['booking_id', 'status', 'checkin_status'])
    # define company dataframe
    df_company_data = [(1, 'company1', '2021-01-01'), (2, 'company2', '2021-01-02')]
    df_company = spark.createDataFrame(df_company_data, ['company_id', 'company_name', 'created_date'])
    # define user data frame
    df_users_data = [(1, 1, '2021-01-01', 'active', False), (2, 2, '2021-01-02', 'active', True)]
    df_users = spark.createDataFrame(df_users_data, ['userID', 'company_id', 'created', 'status', 'demo_user'])
    # load all data to postgres
    Load_to_db(spark, dim_date, df_booking, df_company, df_users)
    # read fact_booking from postgres
    fact_booking = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}/{DBname}") \
        .option("dbtable", "Fact_booking") \
        .option("user", user) \
        .option("password", password) \
        .load()
    # compare df_booking and fact_booking
    assert fact_booking.count() > 0
    # fact_booking columns
    expected_columns = ["user_id", "company_id", "booking_id", "checkingstatus_id", "status", "created_date", "booking_start_date", "booking_end_date"]
    expected_data = [(1, 1, 1, 1, 'status1', '2021-01-01', 'booking_start1', 'booking_end1'),
                     (2, 2, 2, 2, 'status2', '2021-01-02', 'booking_start2', 'booking_end2')]
    # compare fact_booking columns
    assert fact_booking.columns == expected_columns
    assert fact_booking.select(*expected_columns).collect() == expected_data