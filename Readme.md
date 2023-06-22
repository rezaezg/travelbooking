# Conf
before running you should define your sql config in yml file 
# Files Structure

travelbooking/ \
├── data/ # Data directory of booking, users and company \
│   ├── booking.csv/ \
│   ├── company.csv/ \
│   ├── users.csv/ \
├── scripts/ # ETL scripts \ 
│   ├── processing/ \
│   │   │   ├── analysing.py \
│   │   │   ├── cleaning.py \
│   │   │   └── transformation.py \
│   ├── loading/ \
│   │   │   ├── dim_date.py \
│   │   │   └── Load_to_db.py \
│   ├── test/ \
│   │   │   ├── test_ingestion.py \
│   ├── init.py \
├── main.py # main  \
├── docker-compose.yml #docker compose file \
└── Dockerfile #Dockerfile 



# Data Model

###  Fact table
 Fact_booking

### Dimensions
Dim_company, Dim_user, Dim_status, Dim_Checkinstatus, Dim_date


# Queries: 
I write queries in Pyspark and Sql
you can find Pyspark queries in queries.py 
you can find Sql queries in travelbooking.sql

