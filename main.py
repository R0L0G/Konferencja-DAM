import time
import aiohttp
import asyncio
from bs4 import BeautifulSoup as bs
import sqlite3

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


async def sql_insert(item):
    cur.execute('''
        INSERT INTO scrap_data VALUES(?, ?, ?, ?)
    ''',(item[0], item[1], item[2],item[3]))
    conn.commit()


async def on_data_added(data):
    for item in data:
        print(item)
        await sql_insert(item)

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def scrap_bankier_repost_async(link_key, first_page, last_page):
    data = []
    async with aiohttp.ClientSession() as session:
        for page_number in range(first_page, last_page):
            url = urls[link_key].format(page_number)
            await scrap_page(session, url, data, link_key,page_number)
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
    data1=[]
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

    tasks = [
        scrap_bankier_repost_async('alior', 1, 3),
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

    results = await asyncio.gather(*tasks)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Całkowity czas wykonania: {execution_time} sekundy")

if __name__ == "__main__":
    asyncio.run(main())
    cur.close()
    conn.close()
