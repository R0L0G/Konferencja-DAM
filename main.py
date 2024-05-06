from bs4 import BeautifulSoup as bs
import requests

url_alior = 'https://www.bankier.pl/forum/forum_o_alior-bank,6,21,10000001210,{}.html'
url_allegro = 'https://www.bankier.pl/forum/forum_o_allegro,6,21,10000001753,{}.html'
url_assecopol = 'https://www.bankier.pl/forum/forum_o_asseco-poland,6,21,230,{}.html'
url_cdprojekt = 'https://www.bankier.pl/forum/forum_o_cd-projekt,6,21,353,{}.html'
url_cyfrpolsat = 'https://www.bankier.pl/forum/forum_o_cyfrowy-polsat,6,21,10000000415,{}.html'
url_dinopl = 'https://www.bankier.pl/forum/forum_o_dino-polska,6,21,10000001632,{}.html'
url_jsw = 'https://www.bankier.pl/forum/forum_o_jastrzebska-spolka-weglowa,6,21,10000000176,{}.html'
url_kety = 'https://www.bankier.pl/forum/forum_o_kety,6,21,114,{}.html'
url_kghm = 'https://www.bankier.pl/forum/forum_o_kghm,6,21,116,{}.html'
url_kruk = 'https://www.bankier.pl/forum/forum_o_kruk,6,21,10000001000,{}.html'
url_lpp = 'https://www.bankier.pl/forum/forum_o_lpp,6,21,134,{}.html'
url_mbank = 'https://www.bankier.pl/forum/forum_o_mbank,6,21,31,{}.html'
url_orange = 'https://www.bankier.pl/forum/forum_o_orange-polska,6,21,247,{}.html'
url_pekao = 'https://www.bankier.pl/forum/forum_o_pekao,6,21,18,{}.html'
url_pepco = 'https://www.bankier.pl/forum/forum_o_pepco-group-nv,6,21,10000001782,{}.html'
url_pge = 'https://www.bankier.pl/forum/forum_o_pge,6,21,10000000641,{}.html'
url_pknorlen = 'https://www.bankier.pl/forum/forum_o_pkn-orlen,6,21,201,{}.html'
url_pko = 'https://www.bankier.pl/forum/forum_o_pko-bp,6,21,10000000062,{}.html'
url_pzu = 'https://www.bankier.pl/forum/forum_o_pzu,6,21,10000000081,{}.html'
url_santander = 'https://www.bankier.pl/forum/forum_o_banco-santander,6,21,10000001447,{}.html'

data = []

#SAME TYTUŁY
def scrap_bankier(link, first_page , last_page, content):
    for page_number in range(first_page, last_page):
        url = link.format(page_number)
        response = requests.get(url)
        if response.status_code == 200:
            soup = bs(response.content, 'html.parser')

            td_elements_title = soup.find_all('td', class_='threadTitle')
            td_elements_date = soup.find_all('td', class_='createDate textAlignCenter textNowrap')

            for title, date in zip(td_elements_title, td_elements_date):
                link = title.find('a')
                if link:
                    thread_title = link.text.strip()
                    thread_date = date.text.strip()
                    content.append((thread_title, thread_date))
        else:
            print(f"Nie udało się pobrać zawartości strony {url}.")

#DO REPOSTÓW
def scrap_bankier_repost(link, first_page, last_page, content):
    for page_number in range(first_page, last_page):
        url = link.format(page_number)
        response = requests.get(url)
        if response.status_code == 200:
            soup = bs(response.content, 'html.parser')

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
                                link_re(thread_url, content)
        else:
            print(f"Nie udało się pobrać zawartości strony {url}.")

#DO REPOSTÓW C.D
def link_re(thread_url, content):
    thread_url = "https://www.bankier.pl/forum/" + thread_url
    response = requests.get(thread_url)
    if response.status_code == 200:
        soup = bs(response.content, 'html.parser')
        li_elements = soup.find_all('li', class_='level-1')
        for li in li_elements:
            link_element = li.find('a')
            if link_element:
                href = link_element['href']
                re_scrap(href, content)
    else:
        print(f"Nie udało się pobrać zawartości wątku {thread_url}.")

#DO REPOSTÓW C.D
def re_scrap(link_url, content):
    link_url = "https://www.bankier.pl/forum/" + link_url
    response = requests.get(link_url)
    if response.status_code == 200:
        soup = bs(response.content, 'html.parser')
        date_element = soup.find('time', class_='entry-date')
        if date_element:
            date = date_element['datetime']
            content.append(date)
        else:
            print(f"Nie udało się znaleźć daty w wątku {link_url}.")
        box_content = soup.find('div', class_='box810 border1')
        if box_content:
            content_elements = box_content.find('div', class_='boxContent')
            if content_elements:
                list_pom = []
                for content_element in content_elements:
                    content_text = content_element.text.strip()
                    list_pom.append(content_text)
                content.append(list_pom[1])
            else:
                print(
                    f"Nie udało się znaleźć żadnych elementów z klasą 'p enablestyle' w sekcji boxContent w wątku {link_url}.")
        else:
            print(f"Nie udało się znaleźć sekcji boxContent w wątku {link_url}.")
    else:
        print(f"Nie udało się pobrać zawartości wątku {link_url}.")

scrap_bankier_repost(url_alior,1,2,data)
print(data)

