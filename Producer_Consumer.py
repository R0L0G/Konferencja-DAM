import pandas as pd
import asyncio
from queue import Queue
import concurrent.futures
import random
import logging
import sqlite3

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
