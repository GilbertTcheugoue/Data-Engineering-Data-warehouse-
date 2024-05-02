from datetime import datetime
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

def load_data_to_postgres():
    # Your logic to load data into PostgreSQL goes here
    # You can use libraries like psycopg2 or SQLAlchemy
    # Define the database connection parameters
    db_params = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin'
     }

  #Step 2: Creating the PostgreSQL Database

  # Create a connection to the PostgreSQL server
    conn = psycopg2.connect(
    host=db_params['host'],
    database=db_params['database'],
    user=db_params['user'],
    password=db_params['password']
    )
# Create a cursor object
    cur = conn.cursor()

# Set automatic commit to be true, so that each action is committed without having to call conn.committ() after each command
    conn.set_session(autocommit=True)

# Create the 'soccer' database
    cur.execute("CREATE DATABASE soccer")

# Commit the changes and close the connection to the default database
    conn.commit()
    cur.close()
    conn.close()

#Step 3: Loading and Displaying CSV Data

# Connect to the 'soccer' database
    db_params['database'] = 'soccer'
    engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}/{db_params["database"]}')

# Define the file paths for your CSV files
    csv_files = {
    'data': 'C:\Users\GILBERT\Desktop\10_Academy_Cohort_B\Week2\Asssignment\20181024_d1_0830_0900.csv'   
    }

# Load and display the contents of each CSV file to check
    for table_name, file_path in csv_files.items():
        print(f"Contents of '{table_name}' CSV file:")
        df = pd.read_csv(file_path)
    print(df.head(2))  # Display the first few rows of the DataFrame
    print("\n")


# Step 4: Importing CSV Data into PostgreSQL

# Loop through the CSV files and import them into PostgreSQL
    for table_name, file_path in csv_files.items():
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    default_args = {
    'owner': 'your_name',
    'start_date': datetime(2024, 5, 2),
    'retries': 1,
    }

with DAG(
    'data_loading_dag',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,
) as dag:
    # BashOperator example
    bash_task = BashOperator(
        task_id='run_bash_command',
        bash_command='echo "Running data loading script"',
    )

    # PythonOperator example
    python_task = PythonOperator(
        task_id='run_python_script',
        python_callable=load_data_to_postgres,
    )

    # Set task dependencies
    bash_task >> python_task


