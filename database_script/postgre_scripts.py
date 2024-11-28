import psycopg2
import json

from sqlalchemy.sql.ddl import CreateTable

with open("/home/oem/PycharmProjects/RealEstate_Data_Pipeline/secret.json") as f:
   secret =  json.load(f)

db_name = secret["DB_NAME"]
db_pass = secret["DB_PASS"]
user = secret["DB_USER"]


def connect_postgres(self):
    conn = psycopg2.connect(dbname=db_name, user=user, host="localhost", port="5432",
                            password=db_pass)
    print("Connected to Postgres DB")

    return conn

def create_tables(self):
    conn = self.connect_postgres()
    cur = conn.cursor()

    creat_table_query = """
    CREATE TABLE IF NOT EXISTS texas_avg_house_prices(
    RegionID PRIMARY KEY,
    RegionName VARCHAR(255),
    StateName VARCHAR(255),
    County VARCHAR(255),
    )"""
