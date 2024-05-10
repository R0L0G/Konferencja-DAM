import sqlite3

conn = sqlite3.connect(".\\scrap.db")
cur = conn.cursor()
def create_table():
    cur.execute('''
    CREATE TABLE IF NOT EXISTS scrap_data(nazwa_spolki TEXT,
    data_posta TEXT,
     liczba_slow INTEGER,
     text TEXT)
    ''')
    conn.commit()
    cur.close()
    conn.close()

def cleare_table():
    cur.execute('''
    DELETE FROM scrap_data
    ''')
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    pass
    #create_table()
    #cleare_table()