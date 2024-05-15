import time
import aiohttp
import asyncio
from bs4 import BeautifulSoup as bs
import sqlite3
import tracemalloc



urls = {
    'alior': 'https://www.bankier.pl/forum/forum_o_alior-bank,6,21,10000001210,{}.html',
    'allegro': 'https://www.bankier.pl/forum/forum_o_allegro,6,21,10000001753,{}.html',
    'assecopol': 'https://www.bankier.pl/forum/forum_o_asseco-poland,6,21,230,{}.html',
    'cdprojekt': 'https://www.bankier.pl/forum/forum_o_cd-projekt,6,21,353,{}.html',
    'cyfrpolsat': 'https://www.bankier.pl/forum/forum_o_cyfrowy-polsat,6,21,10000000415,{}.html',
    'dinopl': 'https://www.bankier.pl/forum/forum_o_dino-polska,6,21,10000001632,{}.html',
    'jsw': 'https://www.bankier.pl/forum/forum_o_jastrzebska-spolka-weglowa,6,21,10000000176,{}.html',
    'kety': 'https://www.bankier.pl/forum/forum_o_kety,6,21,114,{}.html',
    'kghm': 'https://www.bankier.pl/forum/forum_o_kghm,6,21,116,{}.html',
    'kruk': 'https://www.bankier.pl/forum/forum_o_kruk,6,21,10000001000,{}.html',
    'lpp': 'https://www.bankier.pl/forum/forum_o_lpp,6,21,134,{}.html',
    'mbank': 'https://www.bankier.pl/forum/forum_o_mbank,6,21,31,{}.html',
    'orange': 'https://www.bankier.pl/forum/forum_o_orange-polska,6,21,247,{}.html',
    'pekao': 'https://www.bankier.pl/forum/forum_o_pekao,6,21,18,{}.html',
    'pepco': 'https://www.bankier.pl/forum/forum_o_pepco-group-nv,6,21,10000001782,{}.html',
    'pge': 'https://www.bankier.pl/forum/forum_o_pge,6,21,10000000641,{}.html',
    'pknorlen': 'https://www.bankier.pl/forum/forum_o_pkn-orlen,6,21,201,{}.html',
    'pko': 'https://www.bankier.pl/forum/forum_o_pko-bp,6,21,10000000062,{}.html',
    'pzu': 'https://www.bankier.pl/forum/forum_o_pzu,6,21,10000000081,{}.html',
    'santander': 'https://www.bankier.pl/forum/forum_o_banco-santander,6,21,10000001447,{}.html'
}

conn = sqlite3.connect(".\\scrap.db")
cur = conn.cursor()


async def sql_insert(batch):
    cur.executemany('''
        INSERT INTO scrap_data VALUES(?, ?, ?, ?, ?)
    ''', batch)
    conn.commit()


def select_error():
    cur.execute('''
    SELECT nazwa_spolki, MAX(strona) FROM scrap_data 
    GROUP BY nazwa_spolki
    ''')
    conn.commit()
    lista_stron = cur.fetchall()
    slownik_stron = dict(lista_stron)
    return slownik_stron


async def on_data_added(data):
    batch = []
    for item in data:
        print(tuple(item))
        batch.append(tuple(item))
        if len(batch) == 1000:
            await sql_insert(batch)
        elif len(batch) > 1000:
            batch = []


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def scrap_bankier_repost_async(link_key, first_page, last_page):
    data = []
    async with aiohttp.ClientSession() as session:
        for page_number in range(first_page, last_page + 1):
            url = urls[link_key].format(page_number)
            await scrap_page(session, url, data, link_key, page_number)
    return data


async def scrap_page(session, url, data, link_key,page_number):
    html = await fetch(session, url)
    soup = bs(html, 'html.parser')

    tr_elements = soup.find_all('tr')
    for tr in tr_elements:
        title_cell = tr.find('td', class_='threadTitle')
        if title_cell:
            link = title_cell.a
            if link:
                thread_url = link['href']
                repost_cell = tr.find('td', class_='threadCount')
                if repost_cell:
                    repost_count = int(repost_cell.span.text.strip())
                    if repost_count > 5:
                        await link_re_async(session, thread_url, data, link_key,page_number)

async def link_re_async(session, thread_url, data, link_key,page_number):
    data1 = []
    sql = [link_key]
    sql.append(page_number)
    async with aiohttp.ClientSession() as session:
        thread_url = "https://www.bankier.pl/forum/" + thread_url
        html = await fetch(session, thread_url)
        soup = bs(html, 'html.parser')
        date_element = soup.find('time', class_='entry-date')
        if date_element:
            date = date_element['datetime']
            sql.append(date)
        box_content = soup.find('div', class_='box810 border1')
        if box_content:
            content_elements = box_content.find('div', class_='boxContent')
            if content_elements:
                list_pom = []
                for content_element in content_elements:
                    content_text = content_element.text.strip()
                    list_pom.append(content_text)
                space_count = 0
                for char in list_pom[1]:
                    if char == ' ':
                        space_count += 1
                sql.append(space_count)
                sql.append(list_pom[1])
                data1.append(sql)
                if list_pom[1] != '(wiadomość usunięta przez moderatora)':
                    await on_data_added(data1)
        li_elements = soup.find_all('li', class_='level-1')
        for li in li_elements:
            link_element = li.find('a')
            if link_element:
                href = link_element['href']
                await re_scrap_async(session, href, data, link_key,page_number)

async def re_scrap_async(session, link_url, data, link_key,page_number):
    sql = [link_key]
    sql.append(page_number)
    link_url = "https://www.bankier.pl/forum/" + link_url
    html = await fetch(session, link_url)
    soup = bs(html, 'html.parser')
    date_element = soup.find('time', class_='entry-date')
    if date_element:
        date = date_element['datetime']
        sql.append(date)
    box_content = soup.find('div', class_='box810 border1')
    if box_content:
        content_elements = box_content.find('div', class_='boxContent')
        if content_elements:
            list_pom = []
            for content_element in content_elements:
                content_text = content_element.text.strip()
                list_pom.append(content_text)
            space_count = 0
            for char in list_pom[1]:
                if char == ' ':
                    space_count += 1
            sql.append(space_count)
            sql.append(list_pom[1])
            if list_pom[1] != '(wiadomość usunięta przez moderatora)':
                data.append(sql)
                await on_data_added(data)


async def main():
    start_time = time.time()
    '''
    tasks_old = [
        scrap_bankier_repost_async('alior', 7, 46),
        scrap_bankier_repost_async('allegro', 48, 234),
        scrap_bankier_repost_async('assecopol', 2, 9),
        scrap_bankier_repost_async('cdprojekt', 312, 1161),
        scrap_bankier_repost_async('cyfrpolsat', 98, 158),
        scrap_bankier_repost_async('dinopl', 13, 69),
        scrap_bankier_repost_async('jsw', 227, 1112),
        scrap_bankier_repost_async('kety', 2, 9),
        scrap_bankier_repost_async('kghm', 105, 264),
        scrap_bankier_repost_async('kruk', 4, 10),
        scrap_bankier_repost_async('lpp', 60, 156),
        scrap_bankier_repost_async('mbank', 7, 20),
        scrap_bankier_repost_async('orange', 2, 7),
        scrap_bankier_repost_async('pekao', 10, 39),
        scrap_bankier_repost_async('pepco', 77, 187),
        scrap_bankier_repost_async('pge', 49, 120),
        scrap_bankier_repost_async('pknorlen', 363, 1338),
        scrap_bankier_repost_async('pko', 19, 118),
        scrap_bankier_repost_async('pzu', 13, 49),
        scrap_bankier_repost_async('santander', 1, 2)
    ]

    '''
    #liczba scrapowantych stron :  39+186+7+611+145+56+379+7+159+6+96+13+5+29+110+71+387+99+36+1+11 = 2453  stron na 15.05. godzina 16:24
    # [7, 48, 2, 312, 98, 13, 227, 2, 105, 4, 60, 7, 2, 10, 77, 49, 363, 19, 13, 1] start
    # [21, 119, 9, 374, 158, 69, 288, 9, 157, 10, 116, 20, 7, 39, 95, 49, 380, 31, 19, 1] godzina: 00:05 14.05.2024
    # [29, 125, 9, 375, 158, 69, 289, 9, 160, 10, 122, 20, 7, 39, 98, 49, 382, 34, 21, 1] godzina 00:39
    #[38, 133, 9, 380, 158, 69, 291, 9, 163, 10, 127, 20, 7, 39, 102, 49, 384, 38, 23, 1] godzina 1:12
    #[46, 234, 9, 432, 158, 69, 346, 9, 222, 10, 156, 20, 7, 39, 174, 73, 422, 102, 49, 1] godzina 11:13
    #[46, 234, 9, 462, 158, 69, 378, 9, 250, 10, 156, 20, 7, 39, 187, 93, 455, 118, 49, 1] godzina 16.32
    #[46, 234, 9, 465, 158, 69, 383, 9, 253, 10, 156, 20, 7, 39, 187, 96, 459, 118, 49, 1] godzina 18:35
    #[46, 234, 9, 494, 158, 69, 421, 9, 264, 10, 156, 20, 7, 39, 187, 120, 507, 118, 49, 1] godzina 01:01 15.05.2024
    #[46, 234, 9, 564, 158, 69, 490, 9, 264, 10, 156, 20, 7, 39, 187, 120, 575, 118, 49, 1] godzina 14:24
    #[46, 234, 9, 709, 158, 69, 606, 9, 264, 10, 156, 20, 7, 39, 187, 120, 750, 118, 49, 1] godzina 16:05

    #[46, 234, 9, 1161, 158, 69, 1112, 9, 264, 10, 156, 20, 7, 39, 187, 120, 1338, 118, 49, 2] DONE

    while True:

        firmy = ['alior', 'allegro', 'assecopol', 'cdprojekt', 'cyfrpolsat', 'dinopl', 'jsw', 'kety', 'kghm', 'kruk',
                 'lpp', 'mbank', 'orange', 'pekao', 'pepco', 'pge', 'pknorlen', 'pko', 'pzu', 'santander']
        start_pages = list(select_error().values())
        end_pages = [46, 234, 9, 1161, 158, 69, 1112, 9, 264, 10, 156, 20, 7, 39, 187, 120, 1338, 118, 49, 2]

        tasks2 = [asyncio.create_task(scrap_bankier_repost_async(x, y, z)) for (x, y, z) in zip(firmy, start_pages, end_pages)]
        index = []
        for i in range(0, len(tasks2)):
            if end_pages[i] == start_pages[i]:
                index.append(i)
            else:
                continue
        for j in sorted(index, reverse=True):
            del tasks2[j]
        print(len(tasks2))

        try:
            await asyncio.gather(*tasks2)
        except:
            for t in tasks2:
                t.cancel()
            continue
        else:
            print('DONE')
            break

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Całkowity czas wykonania: {execution_time} sekundy")

if __name__ == "__main__":

    #tracemalloc.start()


    #asyncio.run(main())
    start_pages = list(select_error().values())
    print(start_pages)
    cur.close()
    conn.close()
