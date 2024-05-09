import sqlite3

conn = sqlite3.connect(".\\scrap.db")
cur = conn.cursor()

cur.execute('''
    CREATE TABLE IF NOT EXISTS scrap_data(nazwa_spolki TEXT,
    data_posta TEXT,
     liczba_slow INTEGER,
     text TEXT)
    ''')
conn.commit()

cur.close()
conn.close()