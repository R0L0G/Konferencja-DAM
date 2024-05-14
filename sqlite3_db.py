import sqlite3

def create_table():
    cur.execute('''
    CREATE TABLE IF NOT EXISTS scrap_data(nazwa_spolki TEXT,
    strona INTEGER,
    data_posta TEXT,
     liczba_slow INTEGER,
     text TEXT)
    ''')
    conn.commit()

def cleare_table():
    cur.execute('''
    DELETE FROM scrap_data
    ''')
    conn.commit()

def select():
    cur.execute('''
    SELECT * FROM scrap_data
    ''',)
    conn.commit()
    print(cur.fetchone())

def drop_table():
    cur.execute('''
    DROP TABLE scrap_data
    ''')
    conn.commit()

def select_error():
    cur.execute('''
    SELECT nazwa_spolki, MAX(strona) FROM scrap_data 
    GROUP BY nazwa_spolki
    ''')
    conn.commit()
    lista_stron = cur.fetchall()
    słownik_stron = dict(lista_stron)
    print(słownik_stron)

def select_spolka():
    #dla spolki kruk
    cur.execute('''
    SELECT strona FROM scrap_data
    WHERE strona = 10 AND nazwa_spolki = 'alior'
    ''')
    conn.commit()
    print(cur.fetchall())


def create_table_duplicates_free():
    cur_free.execute('''
    CREATE TABLE IF NOT EXISTS duplicates_free(nazwa_spolki TEXT,
    strona INTEGER,
    data_posta TEXT,
     liczba_slow INTEGER,
     text TEXT)
    ''')
    conn_dupfree.commit()


def duplicats_free_table():
    cur.execute('''
    SELECT DISTINCT * FROM scrap_data
    ''')
    conn.commit()
    data = cur.fetchall()
    cur_free.executemany('''
    INSERT INTO duplicates_free VALUES(?, ?, ?, ?, ?)
       ''', data)
    conn_dupfree.commit()

def clear_duplicates_free_table():
    cur_free.execute('''
        DELETE FROM duplicates_free
        ''')
    conn_dupfree.commit()

def coutn_db():
    cur_free.execute('''
    SELECT nazwa_spolki,data_posta, strona FROM duplicates_free
    where data_posta = '2010-12-10 09:15'
    ''')
    conn_dupfree.commit()
    print(cur_free.fetchall())


if __name__ == "__main__":
    conn = sqlite3.connect(".\\scrap.db")
    cur = conn.cursor()
    conn_dupfree = sqlite3.connect(".\\duplicates_free.db")
    cur_free = conn_dupfree.cursor()
    #create_table()
    #cleare_table()
    #select()
    #drop_table()
    #select_error()
    #select_spolka()

    #create_table_duplicates_free()
    #duplicats_free_table()
    #clear_duplicates_free_table()
    coutn_db()

    cur.close()
    conn.commit()