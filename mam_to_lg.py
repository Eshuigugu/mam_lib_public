import json
import os
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import libgen_search
from transmission_rpc import Client
from requests_toolbelt.multipart.encoder import MultipartEncoder
from time import sleep
import ebooklib
from ebooklib import epub
import re


def get_publisher(epub_filepath, tag='publisher'):
    ebook = epub.read_epub(epub_filepath)
    for v, k in ebook.get_metadata('DC', tag):
        if v != 'UNKNOWN':
            return v


def get_book_text(epub_filepath):
    book = epub.read_epub(epub_filepath)
    book_text = []
    for doc in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
        book_text += [BeautifulSoup(doc.content.decode(), features="lxml").get_text()]
    return '\n'.join(book_text)


def lg_edit_meta(edit_url, title, author, series, isbn, description, publisher):
    meta_data = {
        'metadata_source': 'local',
        'title': title,
        'authors': author,
        # 'language': 'English',
        # 'language_options': 'English',
        'publisher': publisher if publisher else '',
        'series': series,
        # 'year',
        'isbn': isbn,
        # 'asin',
        # 'cover',
        'description': description,
    }

    headers = {'Authorization': lg_public_auth}
    r = sess.get(edit_url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    # copy over existing metadata
    libgen_wanted_fields = ['metadata_source', 'title', 'authors', 'language', 'language_options', 'series', 'year',
                            'isbn', 'asin', 'cover', 'description']
    for k, v in {x['name']: x['value'] for x in soup.find_all('input') if
                 x.has_attr('name') and x['name'] in libgen_wanted_fields and x['value']}.items():
        if not (k in meta_data and meta_data[k]):
            meta_data[k] = v

    data = MultipartEncoder(fields=meta_data)
    headers['Content-type'] = data.content_type
    r = sess.post(edit_url, headers=headers, data=data)
    return r


def upload_ebook_to_libgen(epub_path, libgen_upload_url='https://library.bz/fiction/upload/'):
    files = {
        'file': (os.path.split(epub_path)[-1], open(epub_path, 'rb')),
    }
    headers = {'Authorization': lg_public_auth}
    r = sess.post(libgen_upload_url, headers=headers, files=files)
    return r.url if r.url != libgen_upload_url else None


def download_parse_metadata_and_upload(mam_book):
    if mam_book['category'] not in mam_fiction_cats + mam_nonfiction_cats:
        print(mam_book['category'], 'isnt in fiction or nonfic')
        return
    fiction = mam_book['category'] in mam_fiction_cats

    title = mam_book['title']
    authors = list(json.loads(mam_book['author_info']).values())
    author = authors[0]
    series = ' #'.join(list(json.loads(mam_book['series_info']).values())[0]) if mam_book['series_info'] else ''
    # mam api can give isbn as str or int
    isbn = str(mam_book['isbn']) if mam_book['isbn'] and str(mam_book['isbn'])[0].isdigit() else ''
    isbn = isbn if len(re.sub('[^\d]', '', isbn)) in [10, 13] else ''

    query_str = f'{title} {author}'
    query_str = ' '.join([x for x in query_str.split(' ') if len(x) > 1])  # remove 1 char words
    lg_results = libgen_search.search_lg(query_str, format='epub') if fiction else libgen_search.check_book_on_lg_nonfic(query_str)
    if lg_results:
        print(f'book is on libgen {lg_results}')
        return

    # check if book is on goodreads. this isnt a proper api
    url = f'https://www.goodreads.com/book/auto_complete'
    r = sess.get(url, params={'format': 'json', 'q': query_str},
                 timeout=10)
    get_title_varients = lambda x: {re.sub(' *(?:\:.*|\(.*\))* *$', '', x), re.sub(' *(?:\(.*\))* *$', '', x), x}
    valid_titles = get_title_varients(title.lower())
    goodreads_exact_match = True if r.json() and get_title_varients(r.json()[0]['bookTitleBare'].lower()).intersection(valid_titles) else False
    if not goodreads_exact_match:
        print(f'{query_str} isnt on goodreads {title}')
        return
    if goodreads_exact_match:
        # redo the search with the goodreads title & author
        gr_title = r.json()[0]['bookTitleBare']
        gr_author = r.json()[0]['author']['name']
        if gr_title != title or gr_author != author:
            query_str = f'{gr_title} {gr_author}'
            lg_results = libgen_search.search_lg(query_str, format='epub') if fiction else libgen_search.check_book_on_lg_nonfic(query_str)
            if lg_results:
                print(f'book is on libgen {lg_results}')
                return
    if isbn:  # search nonfic by isbn
        if sess.get(f'https://libgen.rs/json.php?isbn={isbn}&fields=Title,Author', timeout=5).json():
            print(f'book is on libgen')
            return


    get_dl_url = lambda x: \
        f'https://www.myanonamouse.net/tor/download.php/{x}'
    torrent_dl_url = get_dl_url(mam_book['dl'])
    torrent_dl_request = sess.get(torrent_dl_url)
    torrent = trans_client.add_torrent(torrent_dl_request.content)

    # reduce limit by 1 every time we try to download a book
    global limit
    limit -= 1

    # wait for torrent to download
    done_downloading = False
    i = 0
    while not done_downloading:
        torrent = trans_client.get_torrent(torrent.id)
        t_files = torrent.files()
        done_downloading = all([t_file.completed == t_file.size for t_file in t_files])
        sleep(1)
        i += 1
        if i > 60 and not done_downloading:  # wait up to 60 seconds for ebook to download
            print(f'torrent failed to download {t_files}')
            return

    if torrent.download_dir != client_download_dir:
        torrent.move_data(client_download_dir)  # make sure files are where they should be
    # check that downloads are completed and there's only 1 epub
    t_file_names = [x.name for x in t_files if x.name.endswith('.epub')]

    epub_paths = [os.path.join(download_dir, x) for x in t_file_names]
    if not epub_paths:
        print('didnt download any epubs')
        return
    if len(epub_paths) > 1:
        print(f'got too many epubs {epub_paths}')
        return

    epub_path = epub_paths[0]
    if not os.path.exists(epub_path):
        print(f'cant find epub where its expected {epub_path}')
        return

    try:
        book_txt = get_book_text(epub_path)
    except Exception as e:
        print(f'error while reading epub. bad file? {e}')
        return
    if book_txt.count(' ') < 10:
        print(f'book has less than 10 words? {book_txt}')
        return

    publisher = get_publisher(epub_path)
    publisher = publisher if publisher else ''


    upload_url = upload_ebook_to_libgen(epub_path) if fiction else upload_ebook_to_libgen(epub_path, libgen_upload_url='https://library.bz/main/upload/')

    if not upload_url:
        print(f'upload failed for {title}')
    else:
        print(title, epub_path, upload_url)
        r = lg_edit_meta(edit_url=upload_url,
                         title=title, author=', '.join(authors), series=series, isbn=isbn, description=mam_book['description'],
                         publisher=publisher)
        print(datetime.now().isoformat(), 'successful upload?', title, r.status_code, r.url)


def get_books_from_mam(num_to_fetch=50, start_num=0):
    my_cookies = {
        'mam_id': 'https://www.myanonamouse.net/f/t/46853/p/p453334#453334 describes how to get a mam_id'}

    r = sess.get('https://www.myanonamouse.net/jsonLoad.php?snatch_summary', cookies=my_cookies, timeout=20)
    snatch_summary = r.json()
    mam_limit = snatch_summary['unsat']['limit'] - snatch_summary['unsat']['count']
    num_to_fetch = min(num_to_fetch, mam_limit)

    searchin = ['fileTypes']
    date_str_end = datetime.fromtimestamp(datetime.now().timestamp() - 86400 * 4).strftime('%Y-%m-%d')
    mam_books = []
    while len(mam_books) <= num_to_fetch:
        json_dict = {
            "tor": {
                "main_cat": ["14"],  # limit query to ebooks
                "cat": mam_fiction_cats + mam_nonfiction_cats,  # limit categories
                "sortType": 'dateDesc',
                "startNumber": str(start_num),
                "text": '@filetype epub',
                "minSeeders": 2,
                "endDate": date_str_end,  # only look at uploads 4d old to respect MAM rule 2.2
                "browse_lang": ['1'],  # english only
                "minSize": '10',
                "maxSize": str(40*1024),
                "unit": '1024',  # size restrictions limit libgen uploads. must be in range 10Kb - 200 Mb
                # "searchIn": 'mine',
                "srchIn": {
                    x: "true" for x in (searchin if type(searchin) == list else [searchin])
                }
            },
            "dlLink": "true",
            "isbn": "true",
            "description": "true"
        }

        r = sess.post('https://www.myanonamouse.net/tor/js/loadSearchJSONbasic.php', json=json_dict,
                      cookies=my_cookies, timeout=20)
        sleep(1)
        start_num += r.json()['perpage']
        # it's easier to handle torrents with only 1 file. should be an epub
        mam_books += [x for x in r.json()['data'] if x['numfiles'] == 1 and x['id'] not in upload_blacklist]
        if start_num >= r.json()['found']:
            print(f'no more MAM books to process')
            break
    return mam_books, mam_limit


mam_fiction_cats = [60, 102, 65, 109, 70, 64, 94, 62, 68, 63, 69, 112, 66, 67]
mam_nonfiction_cats = [71, 72, 90, 101, 107, 74, 76, 77, 115, 91, 78, 80, 92]
unwanted_cats = [61, 79]

transmission_username, transmission_password = 'username:password'.split(':')
transmission_ip = '127.0.0.1'
transmission_port = 9091  # 9091 is transmission's default port
lg_public_auth = 'Basic Z2VuZXNpczp1cGxvYWQ='  # not a secret. it's genesis:upload base64 encoded

trans_client = Client(host=transmission_ip, port=transmission_port, username=transmission_username, password=transmission_password)
client_download_dir = '/download/'  # path where torrent client will download the ebooks to
download_dir = '/download/'  # path that the computer running this script will find the downloaded ebooks at
limit = 50

sess = requests.Session()

if '__file__' in globals():
    pwd = os.path.split(__file__)[:-1]
else:
    pwd = []
blacklist_filepath = os.path.join(*pwd, 'mam_id_blacklist.txt')
upload_blacklist = []
if os.path.exists(blacklist_filepath):
    with open(blacklist_filepath, 'r') as f:
        upload_blacklist = [int(x.strip()) for x in f.readlines()]


if __name__ == '__main__':
    mam_books, mam_limit = get_books_from_mam(num_to_fetch=limit)
    # limit is decremented in download_parse_metadata_and_upload function when books are downloaded from MAM
    limit = min(limit, mam_limit)

    for mam_book in mam_books:
        if mam_book['id'] in upload_blacklist:
            print(f'{mam_book["title"]} in blacklist')
            continue
        else:
            with open(blacklist_filepath, 'a') as f:
                f.write(str(mam_book['id']) + '\n')

        download_parse_metadata_and_upload(mam_book)
        if limit <= 0:
            print(f'reached upload limit')
            break

