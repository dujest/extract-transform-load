from typing import Union, Dict
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import numpy as np

def extract_transform_load(db_params: Dict[str, Union[str, int]],                        
                          file_path: str,
                          table_name: str,
                          create_database: bool = False,
                          new_database_name: Union[str, None] = None) -> None:

    if create_database:
        conn = psycopg2.connect(
            database=db_params['db_name'],
            user=db_params['db_user'],
            password=db_params['db_password'],
            host=db_params['db_host'],
            port=db_params['db_port']
        )

        cur = conn.cursor()

        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cur.execute(f'DROP DATABASE IF EXISTS {new_database_name};')

        cur.execute(f'CREATE DATABASE {new_database_name};')

        cur.close()

        conn.close()

        db_params['db_name'] = new_database_name

    conn = psycopg2.connect(
        database=db_params['db_name'],
        user=db_params['db_user'],
        password=db_params['db_password'],
        host=db_params['db_host'],
        port=db_params['db_port']
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()

    df = pd.read_excel(file_path, sheet_name=0, header=5, usecols=lambda column: column.strip(' 0123456789') not in ['', 'Unnamed:'])

    df = df.iloc[:-1]

    df = df.replace(np.nan, None)
    
    columns = ["_".join(s.strip().rstrip("%").split()) for s in df.columns.tolist()]

    cur.execute(f'DROP TABLE IF EXISTS {table_name};')

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns[0]} VARCHAR(50) NOT NULL,
        {columns[1]} VARCHAR(50),
        {columns[2]} INT,
        {columns[3]} INT,
        {columns[4]} FLOAT,
        {columns[5]} FLOAT,
        {columns[6]} FLOAT,
        {columns[7]} FLOAT,
        {columns[8]} FLOAT,
        {columns[9]} FLOAT,
        {columns[10]} FLOAT,
        {columns[11]} FLOAT
        );
    """)

    values = df.values

    columns_string = ', '.join(columns)

    from psycopg2.extras import execute_values

    execute_values(cur,
                   f"INSERT INTO {table_name} ({columns_string}) VALUES %s;",
                   values)

    cur.close()
    conn.close()
