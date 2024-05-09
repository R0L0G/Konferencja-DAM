import time
import aiohttp
import asyncio
from bs4 import BeautifulSoup as bs

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


async def on_data_added(data):
    for item in data:
        print(item)

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def scrap_bankier_repost_async(link_key, first_page, last_page):
    data = []
    async with aiohttp.ClientSession() as session:
        for page_number in range(first_page, last_page):
            url = urls[link_key].format(page_number)
            await scrap_page(session, url, data, link_key)
    return data

async def scrap_page(session, url, data, link_key):
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
                        await link_re_async(session, thread_url, data, link_key)

async def link_re_async(session, thread_url, data, link_key):
    async with aiohttp.ClientSession() as session:
        thread_url = "https://www.bankier.pl/forum/" + thread_url
        html = await fetch(session, thread_url)
        soup = bs(html, 'html.parser')
        li_elements = soup.find_all('li', class_='level-1')
        for li in li_elements:
            link_element = li.find('a')
            if link_element:
                href = link_element['href']
                await re_scrap_async(session, href, data, link_key)

async def re_scrap_async(session, link_url, data, link_key):
    sql = [link_key]
    link_url = "https://www.bankier.pl/forum/" + link_url
    html = await fetch(session, link_url)
    soup = bs(html, 'html.parser')
    date_element = soup.find('time', class_='entry-date')
    if date_element:
        date = date_element['datetime']
        sql.append(date)
        print(f"Zebrałem datę: {date} dla linku: {link_url}")
    box_content = soup.find('div', class_='box810 border1')
    if box_content:
        content_elements = box_content.find('div', class_='boxContent')
        if content_elements:
            list_pom = []
            for content_element in content_elements:
                content_text = content_element.text.strip()
                list_pom.append(content_text)
            sql.append(list_pom[1])
    data.append(sql)
    await on_data_added(data)

async def main():
    start_time = time.time()

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

    results = await asyncio.gather(*tasks)

    data_alior = results[0]
    data_allegro = results[1]
    data_assecopol = results[2]
    data_cdprojekt = results[3]
    data_cyfrpolsat = results[4]
    data_dinopl = results[5]
    data_jsw = results[6]
    data_kety = results[7]
    data_kghm = results[8]
    data_kruk = results[9]
    data_lpp = results[10]
    data_mbank = results[11]
    data_orange = results[12]
    data_pekao = results[13]
    data_pepco = results[14]
    data_pge = results[15]
    data_pknorlen = results[16]
    data_pko = results[17]
    data_pzu = results[18]
    data_santander = results[19]

    print("Data dla results:", results)
    print("Data dla Aliora:", data_alior)
    print("Data dla Allegro:", data_allegro)
    print("Data dla Asseco Poland:", data_assecopol)
    print("Data dla CD Projekt:", data_cdprojekt)
    print("Data dla Cyfrowy Polsat:", data_cyfrpolsat)
    print("Data dla Dino Polska:", data_dinopl)
    print("Data dla Jastrzębska Spółka Węglowa:", data_jsw)
    print("Data dla Kety:", data_kety)
    print("Data dla KGHM:", data_kghm)
    print("Data dla Kruk:", data_kruk)
    print("Data dla LPP:", data_lpp)
    print("Data dla mBank:", data_mbank)
    print("Data dla Orange Polska:", data_orange)
    print("Data dla Pekao:", data_pekao)
    print("Data dla Pepco Group NV:", data_pepco)
    print("Data dla PGE:", data_pge)
    print("Data dla PKN Orlen:", data_pknorlen)
    print("Data dla PKO Bank Polski:", data_pko)
    print("Data dla PZU:", data_pzu)
    print("Data dla Santander Bank Polska:", data_santander)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Całkowity czas wykonania: {execution_time} sekundy")

if __name__ == "__main__":
    asyncio.run(main())
