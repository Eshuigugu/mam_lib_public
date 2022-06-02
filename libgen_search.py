import requests
from bs4 import BeautifulSoup
from time import sleep


MIRROR_SOURCES = ["GET", "Cloudflare", "IPFS.io", "Infura"]
sess = requests.Session()


def send_request(query, base_url='https://libgen.rs/fiction/', format=''):
    search_params = {'q': query,
    'criteria': '',
    'language': '',
    'format': format}
    return sess.get(base_url, params=search_params, timeout=60)


def check_book_on_lg_nonfic(query, sleep_time=10):
    url = 'https://libgen.rs/search.php'
    params = {'req': query}
    r = sess.get(url, params=params)
    sleep(sleep_time)
    soup = BeautifulSoup(r.text, 'html.parser')
    results = soup.find('table', align="center").find_all('tr')[1:]
    return bool(results)


def search_lg(query, format='', sleep_time=10):
    sleep(sleep_time)
    try:r = send_request(query, format=format)
    except:return
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
    else:
        sleep(20)
        return

    if soup.thead is None:
        return
    column_names = [x.text.strip() for x in soup.thead.find_all('td')]
    tbody = soup.find('tbody')
    results = []

    for soup_result in tbody.find_all('tr'):
        result_dict = {}
        for i, x in enumerate(soup_result('td')):
            if column_names[i] == 'Author(s)':
                result_dict['authors'] = x.text.strip()
                continue
            if column_names[i] == 'Title':
                result_dict['title'] = x.text.split('\n')[0].strip()
                continue
            if column_names[i] == 'Series':
                result_dict['series'] = x.text.strip()
                continue
            if column_names[i] == 'File':
                result_dict['Extension'], result_dict['Size'] = x.text.strip().replace('\xa0', ' ').split(' / ')
                result_dict['Extension'] = result_dict['Extension'].lower()
                continue
            if column_names[i] == 'Mirrors':
                mirror_urls = [y.get('href') for y in x('a')]
                result_dict['urls'] = mirror_urls
                continue
        results.append(result_dict)
    return results


def resolve_download_links(item):
    mirror_1 = item["urls"][0]
    page = requests.get(mirror_1)
    soup = BeautifulSoup(page.text, "html.parser")
    links = soup.find_all("a", string=MIRROR_SOURCES)
    download_links = {link.string: link["href"] for link in links}
    return download_links


if __name__ == '__main__':
    title, author = 'The Picture of Dorian Gray by Oscar Wilde'.split(' by ')  # this books is public domain
    lg_results = search_lg(f'{title} {author}')
    print(lg_results)

