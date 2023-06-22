from pyspark.sql.functions import split, to_date


def transformation(df_booking, df_company, df_users):
    # clean booking df
    df_booking = transformation_booking(df_booking)
    # clean company df
    df_company = transformation_company(df_company)
    # clean users df
    df_users = transformation_users(df_users)
    return df_booking, df_company, df_users


def transformation_booking(df_booking):
    # transform datetime to date
    df_booking = df_booking.withColumn("created_date", to_date(split(df_booking.created_at, '\.')[0])) \
                         .withColumn("booking_start_date", to_date(split(df_booking.booking_start_time, '\.')[0])) \
                         .withColumn("booking_end_date", to_date(split(df_booking.booking_end_time, '\.')[0])) 
    df_booking = df_booking.drop("created_at", "booking_start_time", "booking_end_time")   
    return df_booking


def transformation_company(df_company):
    # transform datetime to date
    df_company = df_company.withColumn("created_date", to_date(split(df_company.created_at, '\.')[0])) 
    return df_company


def transformation_users(df_users):
    # transform rn column to userID and datetime to date
    df_users = df_users.withColumn("userID", (df_users.rn)) \
        .withColumn("created", to_date(split(df_users.created_at, '\.')[0])) 
    df_users = df_users.drop("created_at")
    df_users.show()
    return df_users
