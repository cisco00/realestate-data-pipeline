import psycopg2
import json
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from etldataprocessing.zillow_data_cleaning import implementing_func
from etldataprocessing.data_transformation_scripts import create_state_dfs, processed_quarters
from etldataprocessing.zillow_data_extraction import Zillow_Estate

def extract_method():
    extract_data = Zillow_Estate()
    extract_data.main()

path_direct2 = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/spark_query/dataframe1.csv"

with open("/home/oem/PycharmProjects/RealEstate_Data_Pipeline/package.json") as f:
   secret =  json.load(f)

db_name = secret["DB_NAME"]
db_pass = secret["DB_PASS"]
user = secret["DB_USER"]
email = secret["DB_EMAIL"]


def connect_postgres(db_name, db_pass, user):

    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=db_pass,
            host="localhost",
            port=5432
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise


def remove_duplicate_columns(df):
    if len(df.columns) != len(set(df.columns)):
        print(f"Removing duplicate columns: {df.columns[df.columns.duplicated()].tolist()}")
    return df.loc[:, ~df.columns.duplicated()]


def create_tables(df, table_name, db_name, db_pass, user):
    df = remove_duplicate_columns(df)

    conn = connect_postgres(db_name, db_pass, user)
    cur = conn.cursor()

    dtype_mapping = {
        'object': 'VARCHAR',
        'int64': 'INTEGER',
        'float64': 'REAL'
    }

    # Build column definitions for the table
    columns = []
    for col in df.columns:
        col_dtype = str(df[col].dtypes)
        pg_dtype = dtype_mapping.get(col_dtype, 'TEXT')
        col_quoted = f'"{col}"'
        columns.append(f"{col_quoted} {pg_dtype}")

    columns_query = ", ".join(columns)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns_query}
    );
    """

    try:
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating table '{table_name}': {e}")
    finally:
        print("Table created successfully on postgres database.")
        cur.close()
        conn.close()


def etl_processing(df_path):
    df1_clean, df2_clean = implementing_func(path_direct2)

    #Calculating mean quarterly house prices
    final_df = processed_quarters(df1_clean)

    #Data transformation
    state_df = create_state_dfs(final_df)

    # Access specific state DataFrames
    tx_df = state_df["TX"]
    fl_df = state_df["FL"]
    oh_df = state_df["OH"]

    return tx_df, fl_df, oh_df


def loading_data_to_postgres():
    tx_df, fl_df, oh_df = etl_processing(path_direct2)

    table_names = ["TexasTable", "FloridaTable", "OhioTable"]
    datasets = [tx_df, fl_df, oh_df]
#
    for dataset, table_name in zip(datasets, table_names):
        create_tables(dataset, table_name, db_name, db_pass, user)


#Creating airflow dag for pipeline
defaults_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "email": [email],
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "zillow_etl_pipeline_dag",
    default_args=defaults_args,
    description="zillow real estate house prices data pipeline",
    schedule_interval="0 0 * * *",
    start_date=airflow.utils.dates.days_ago(2),
    catchup=False
)

data_extract = PythonOperator(
    task_id="data_extract",
    python_callable=extract_method,
    dag=dag)

etl_task = PythonOperator(
    task_id="etl_task",
    python_callable=loading_data_to_postgres,
    dag=dag
)

data_extract >> etl_task