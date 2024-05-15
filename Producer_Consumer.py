import asyncio
from queue import Queue
import logging
import sqlite3
from main import scrap_bankier_repost_async



format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.DEBUG, datefmt="%H:%M:%S")

conn = sqlite3.connect(".\\scrap.db")
cur = conn.cursor()

def producer(queue, url):
    results = 0
    logging.debug('putting %i' % results)
    queue.put(results)

def consumer(queue, event, database):
    while not event.is_set() or queue.qsize() != 0:
        try:
            results = queue.get(timeout=2)
            logging.info('Consumer: %i queue size: %i' % (results, queue.qsize()))
            cur.execute('''''')
        except Exception as e:
            logging.debug(e)
    logging.info('consumer exiting')


if __name__ == '__main__':
    queue = Queue(5)
    event = asyncio.Event()

    tasks = [
        scrap_bankier_repost_async('alior', 1, 2),
        scrap_bankier_repost_async('allegro', 1, 2),
        scrap_bankier_repost_async('assecopol', 1, 2),
        scrap_bankier_repost_async('cdprojekt', 1, 2),
        scrap_bankier_repost_async('cyfrpolsat', 1, 2),
        scrap_bankier_repost_async('dinopl', 1, 2),
        scrap_bankier_repost_async('jsw', 1, 2),
        scrap_bankier_repost_async('kety', 1, 2),
        scrap_bankier_repost_async('kghm', 1, 2),
        scrap_bankier_repost_async('kruk', 1, 2),
        scrap_bankier_repost_async('lpp', 1, 2),
        scrap_bankier_repost_async('mbank', 1, 2),
        scrap_bankier_repost_async('orange', 1, 2),
        scrap_bankier_repost_async('pekao', 1, 2),
        scrap_bankier_repost_async('pepco', 1, 2),
        scrap_bankier_repost_async('pge', 1, 2),
        scrap_bankier_repost_async('pknorlen', 1, 2),
        scrap_bankier_repost_async('pko', 1, 2),
        scrap_bankier_repost_async('pzu', 1, 2),
        scrap_bankier_repost_async('santander', 1, 2)
        ]

#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


async def sql_insert(batch):
    cur.executemany('''
        INSERT INTO scrap_data VALUES(?, ?, ?, ?, ?)
    ''', batch)
    conn.commit()


async def on_data_added(data):
    batch = []
    for item in data:
        print(tuple(item))
        batch.append(tuple(item))
        if len(batch) == 100:
            await sql_insert(batch)
        elif len(batch) > 100:
            batch = []


