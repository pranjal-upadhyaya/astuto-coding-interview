import sqlite3

con = sqlite3.connect("task.db")
cur = con.cursor()

with open("model/schema.sql", "r", encoding="utf-8") as schema_file:
    cur.executescript(schema_file.read())
    con.commit
