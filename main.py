from dotenv import load_dotenv
import os
from etl import extract_transform_load

load_dotenv()

db_params = {
    'db_name': os.getenv('DB_NAME'),
    'db_user': os.getenv('DB_USER'),
    'db_password': os.getenv('DB_PASSWORD'),
    'db_host': os.getenv('DB_HOST'),
    'db_port': os.getenv('DB_PORT'),
}

excel_file_path = './SpaceNK_2.0.xlsx'
table_name = 'sales_data'
new_db_name = 'sales_db'

extract_transform_load(
    db_params,
    file_path=excel_file_path,
    table_name=table_name,
    create_database=True,
    new_database_name=new_db_name
)
