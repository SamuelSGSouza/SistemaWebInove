
import pymysql
from pymysql.cursors import SSDictCursor


DB_HOST = "177.39.236.242"
DB_PORT = int("3306")
DB_USER = "invertusPortabilidade"
DB_PASSWORD = "f3LlWU21yi4jHOXE2e"
DB_NAME = "lnp"

conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset="utf8mb4",
            cursorclass=SSDictCursor,
            connect_timeout=10000,
            read_timeout=12000,
        )

with conn.cursor() as cursor:
    cursor.execute("SHOW TABLES")
    for row in cursor:
        print(row)