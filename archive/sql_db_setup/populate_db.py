import pymysql
from pymysql.constants import CLIENT
import sqlparse
import json

def run_query(sql_file, cursor, conn):
    with open(sql_file, "r") as f:
        content = f.read()
    
    queries = sqlparse.split(content)
    queries = [query.strip() for query in queries if query.strip()]

    for query in queries:
        cursor.execute(query)
    conn.commit()

    print(f"Query {sql_file} executed")

    cursor.fetchall()

def database_connect(db_user, db_pass, db_host, db_port):
    conn = pymysql.connect(user=db_user, password=db_pass, host=db_host, port=db_port, client_flag=CLIENT.MULTI_STATEMENTS)
    print("Database connection established")
    cursor = conn.cursor()
    return conn, cursor

def db_conn_cleanup(cursor, conn):
    cursor.close()
    conn.close()
    print("Database connection and cursor closed")

if __name__ == "__main__":
    with open("auth.json", "r") as f:
        config_data = json.load(f)
        db_host = config_data['host']
        db_port = config_data['port']
        db_user = config_data['user']
        db_pass = config_data['password']
        db_schema = config_data['schema']

    sql_files = []
    with open("data_insert.json", "r") as f:
        sql_files = json.load(f)
    
    db_conn, db_cursor = database_connect(db_user, db_pass, db_host, db_port)

    db_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_schema};")
    db_conn.commit()
    db_cursor.execute(f"USE {db_schema};")
    db_cursor.fetchall()

    for file in sql_files:
        run_query(file, db_cursor, db_conn)

    db_conn_cleanup(db_cursor, db_conn)
