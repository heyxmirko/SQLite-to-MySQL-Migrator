import sqlite3
import mysql.connector
from mysql.connector import errorcode

BATCH_SIZE = 10000

def create_mysql_connection():
    return mysql.connector.connect(
        user='',
        password='',
        host='',
        database='',
        connection_timeout=600
    )

sqlite_conn = sqlite3.connect("database.db")
sqlite_cursor = sqlite_conn.cursor()

sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = sqlite_cursor.fetchall()

for table in tables:
    table_name = table[0]
    print(f"Processing table: {table_name}")

    sqlite_cursor.execute(f"SELECT sql FROM sqlite_master WHERE name='{table_name}'")
    create_table_sql = sqlite_cursor.fetchone()[0]

    create_table_sql = create_table_sql.replace("AUTOINCREMENT", "AUTO_INCREMENT")
    create_table_sql = create_table_sql.replace("INTEGER PRIMARY KEY", "INT PRIMARY KEY AUTO_INCREMENT")

    mysql_conn = create_mysql_connection()
    mysql_cursor = mysql_conn.cursor()

    mysql_cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

    # Drop table if exists in MySQL
    try:
        mysql_cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    except mysql.connector.Error as err:
        print(f"Error dropping table {table_name}: {err}")
        continue

    try:
        mysql_cursor.execute(create_table_sql)
    except mysql.connector.Error as err:
        print(f"Error creating table {table_name}: {err}")
        continue

    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    rows = sqlite_cursor.fetchall()

    column_names = [description[0] for description in sqlite_cursor.description]
    column_names_str = ', '.join(column_names)
    placeholders = ', '.join(['%s'] * len(column_names))

    insert_sql = f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders})"
    try:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            mysql_cursor.executemany(insert_sql, batch)
            mysql_conn.commit()
            print(f"Inserted batch {i // BATCH_SIZE + 1} for table {table_name}")
    except mysql.connector.Error as err:
        print(f"Error inserting data into {table_name}: {err}")

    mysql_cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    mysql_conn.close()

sqlite_conn.close()

print("Migration completed successfully.")