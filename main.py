from pyspark.sql import SparkSession
from scripts.processing.analysing import analysing
from scripts.processing.cleaning import cleaning
from scripts.processing.transformation import transformation
from scripts.Loading.Load_to_db import Load_to_db
from scripts.Loading.dim_date import dimdate
from scripts.queries.queries import reports
from scripts.test.test_ingestion import TestLoadToDB


def main():
    # files path
    booking_path = "data/booking.csv"
    company_path = "data/company.csv"
    users_path = "data/users.csv" 
    # start sparksession 
    spark = SparkSession.builder.appName("travelbooking").getOrCreate()

    try:       
        # analysing data
        analysing(spark, booking_path, company_path, users_path)

        # cleaning each data file and get dataframe
        df_booking, df_company, df_users = cleaning(spark, booking_path, company_path, users_path)

        # sometransformation and get back dataframe
        df_booking, df_company, df_users = transformation(df_booking, df_company, df_users)

        # create dim date
        start_date = "2022-10-01"
        end_date = "2023-05-30"        
        df_date = dimdate(spark, start_date, end_date)

        # save dimensions and fact to db
        Load_to_db(df_date, df_booking, df_company, df_users)
        
        # queries
        reports(spark)

        # test
        TestLoadToDB(spark)
         
    except Exception as e:
        # print errors
        print(e)

    finally:
        # Stop the SparkSession
        spark.stop()
        

if __name__ == "__main__":
    main()