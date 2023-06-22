from pyspark.sql.functions import count, when, isnull,col


def analysing(spark,booking_path, company_path, users_path):
  # analysing booking df
  print("**** analysing booking data ****")
  analysis_booking(spark, booking_path)
  # analysing company df
  print("**** analysing company data ****")
  analysis_company(spark, company_path)
  # analysing users df
  print("**** analysing users data ****")
  analysis_users(spark, users_path)


def analysis_booking(spark, booking_path):
  # reading booking file
  df_booking = spark.read.format("csv").option("header","true").load(booking_path)
  # checking null data
  df_booking.select([count(when(isnull(c) | col(c).isNull(), c)).alias(c) for c in df_booking.columns]).show()
  # checking duplicate for user_id and booking_id
  df_booking \
      .groupby(['user_id', 'booking_id']) \
      .count() \
      .where('count > 1') \
      .show()
  # schema checking
  df_booking.printSchema()
  df_booking.select("status").distinct().show()
  df_booking.select("checkin_status").distinct().show()


def analysis_company(spark, company_path):
  # reading company file
  df_company = spark.read.format("csv").option("header","true").load(company_path)
  # null data
  df_company.select([count(when(isnull(c) | col(c).isNull(), c)).alias(c) for c in df_company.columns]).show()
  # checking duplicate for company_id, company_name
  df_company \
      .groupby(['company_id', 'company_name']) \
      .count() \
      .where('count > 1') \
      .show()
  # schema checking
  df_company.printSchema()


def analysis_users(spark, users_path):
  # reading users file
  df_users = spark.read.format("csv").option("header","true").load(users_path)
  # null data
  df_users.select([count(when(isnull(c) | col(c).isNull(), c)).alias(c) for c in df_users.columns]).show()
  # checking duplicate for rn
  df_users \
      .groupby(['rn']) \
      .count() \
      .where('count > 1') \
      .show()
  # schema checking
  df_users.printSchema()