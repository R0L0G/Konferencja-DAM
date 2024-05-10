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

def select():
    cur.execute('''
    SELECT * FROM scrap_data
    ''',)
    conn.commit()
    print(cur.fetchone())
    cur.close()
    conn.close()

def drop_table():
    cur.execute('''
    DROP TABLE scrap_data
    ''')
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    pass
    #create_table()
    #cleare_table()
    select()
    #drop_table()