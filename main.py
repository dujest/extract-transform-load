from dotenv import load_dotenv
import os
from etl import extract_transform_load

# Load environment variables from a .env file
load_dotenv()

# Create a dictionary with database connection parameters
db_params = {
    "db_name": os.getenv("DB_NAME"),
    "db_user": os.getenv("DB_USER"),
    "db_password": os.getenv("DB_PASSWORD"),
    "db_host": os.getenv("DB_HOST"),
    "db_port": os.getenv("DB_PORT"),
}

# Specify the path to the Excel file
excel_file_path = "./SpaceNK_2.0.xlsx"

# Specify the name of the table in the database
table_name = "sales_data"

# Specify the name of the new database to be created
new_db_name = "sales_db"

# Call the extract_transform_load function to perform ETL process
extract_transform_load(
    db_params,
    file_path=excel_file_path,
    table_name=table_name,
    create_database=True,  # Create a new database
    new_database_name=new_db_name,
)
