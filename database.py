import sqlite3

conn = sqlite3.connect("database.db")
cursor = conn.cursor()
sql = """
CREATE TABLE IF NOT EXISTS users(
username TEXT PRIMARY KEY NOT NULL,
password TEXT NOT NULL
)"""
cursor.execute(sql)
cursor.execute("SELECT * FROM users")
res = cursor.fetchall()
print(res)
conn.commit()
conn.close()