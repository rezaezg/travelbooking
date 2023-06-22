from pyspark.sql.functions import monotonically_increasing_id
import os

# database configs
host = os.environ.get('POSTGRES_HOST')
port = os.environ.get('POSTGRES_PORT')
user = os.environ.get('POSTGRES_USER')
password = os.environ.get('POSTGRES_PASSWORD')
DBname = os.environ.get('POSTGRES_DB')


def Load_to_db(dim_date, df_booking, df_company, df_users):
    # save dim date into postgres
    save_dim_to_db(dim_date, host, DBname, user, password, "dim_date")
    # defining dim user
    dim_user = df_users.select("userID", "company_id", "created") \
                       .filter(df_users.status == "active") \
                       .filter("demo_user == FALSE")
    # save dim user into postgres
    save_dim_to_db(dim_user, host, DBname, user, password, "dim_user") 
    # defining dim company
    dim_company = df_company.select("company_id", "company_name", "created_date") \
                            .filter(df_company.status == "active")
    save_dim_to_db(dim_company, host, DBname, user, password, "dim_company")
    # select checkingstatus and distint it's value
    distinct_checkingstatus_values = df_booking.select("checkin_status").distinct()
    # add pervious value to new dataframe and define new column
    df_checkingstatus = distinct_checkingstatus_values.withColumnRenamed("checkin_status", "checkingstatus_name")
    # add ID column to new dataframe
    dim_checkingstatus = df_checkingstatus.withColumn("checkingstatus_ID", monotonically_increasing_id())
    # save dim checkingstatus to postgres
    save_dim_to_db(dim_checkingstatus, host, DBname, user, password, "dim_checkingstatus")
    # select status and distint it's value
    distinct_status_values = df_booking.select("status").distinct()
    # add pervious value to new dataframe and define new column
    df_status = distinct_status_values.withColumnRenamed("status", "status_name")
    # add ID column to new dataframe
    dim_status = df_status.withColumn("status_ID", monotonically_increasing_id())
    # save dim status to postgres
    save_dim_to_db(dim_status, host, DBname, user, password, "dim_status")
    # Left joining booking and status
    df_booking = df_booking.join(dim_status, df_booking.status == dim_status.status_name, 'left')
    df_booking = df_booking.drop('status')
    # Left joining booking and checkingstatus
    df_booking = df_booking.join(dim_checkingstatus, df_booking.checkin_status == dim_checkingstatus.checkingstatus_name, 'left')
    # drop checkingstatus_name 
    df_booking = df_booking.drop('checkingstatus_name')
    # joining user and booking
    df_booking = df_booking.join(df_users, df_booking.user_id == df_users.userID, "inner")
    # selecting fact columns
    fact_booking = df_booking.select("user_id", "company_id", "booking_id", "checkingstatus_id", "status", "created_date","booking_start_date","booking_end_date")
    # save fact table to postgres
    save_dim_to_db(fact_booking, host, DBname, user, password, "Fact_booking")


def save_dim_to_db(df, host, DBname, user, password, table_name):
    # write to postgres
    df.write.format("jdbc").option("url", f"jdbc:postgresql://{host}/{DBname}") \
            .option("dbtable", table_name).option("user", user) \
            .option("password", password).mode("overwrite").save()

