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
import logging
import sys
import postmarkup
from html2bbcode.parser import HTML2BBCode


logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s',
                              '%m-%d-%Y %H:%M:%S')

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

file_handler = logging.FileHandler('logs.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stdout_handler)


def get_publisher(epub_filepath, tag='publisher'):
    ebook = epub.read_epub(epub_filepath)
    # print(ebook.metadata)
    for v, k in ebook.get_metadata('DC', tag):
        if v != 'UNKNOWN':
            return v


def get_book_text(epub_filepath):
    book = epub.read_epub(epub_filepath)
    book_text = []
    for doc in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
        book_text += [BeautifulSoup(doc.content.decode(), features="lxml").get_text()]
    return '\n'.join(book_text)


def lg_edit_meta(edit_url, title, author, series, isbn, description, publisher, asin):
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
        'asin': asin,
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


def query_book_databases(query_str):
    # check if book is on goodreads or google books
    url = f'https://www.goodreads.com/book/auto_complete'
    r = sess.get(url, params={'format': 'json', 'q': query_str},
                 timeout=10)
    if r.json():
        gr_title = r.json()[0]['bookTitleBare']
        gr_author = r.json()[0]['author']['name']
        yield gr_title, gr_author

    url = 'https://www.googleapis.com/books/v1/volumes'
    r = sess.get(url, params={'q': query_str}, timeout=10)
    if r.json()['totalItems']:
        gb_volume = r.json()['items'][0]['volumeInfo']
        if 'authors' in gb_volume:
            gb_title = gb_volume['title']
            gb_author = gb_volume['authors'][0]
            yield gb_title, gb_author


def download_parse_metadata_and_upload(mam_book):
    needed_metadata = ['title', 'author_info']
    for x in needed_metadata:  # handle mam books that are missing metadata
        if not (x in mam_book and mam_book[x]):
            print(f'book is missing {x}', mam_book)
            return
    # * growls * why does mam give me bbcode inside html * tail swishes angrily *
    if mam_book['description'] != postmarkup.strip_bbcode(mam_book['description']):
        mam_book['description'] = postmarkup.render_bbcode(html_to_bbcode_parser.feed(mam_book['description'])).replace('<br/><br/>', '<br/>')
    if 'myanonamouse.net' in mam_book['description'] or mam_book['description'] != postmarkup.strip_bbcode(mam_book['description']):
        print(f'bad description', mam_book['description'])
        mam_book['description'] = ''

    if mam_book['category'] not in mam_fiction_cats + mam_nonfiction_cats:
        print(mam_book['category'], 'isnt in fiction or nonfic')
        return
    fiction = mam_book['category'] in mam_fiction_cats

    title = mam_book['title']
    authors = list(json.loads(mam_book['author_info']).values())
    series = ', '.join([' '.join(x).strip() for x in list(json.loads(mam_book['series_info']).values())]) if mam_book['series_info'] else ''

    asin = None
    # mam api can give isbn as str or int
    mam_book['isbn'] = str(mam_book['isbn'])
    if mam_book['isbn'].lower().startswith('asin:'):
        asin = mam_book['isbn'][5:]
    isbn = re.sub('[^\d]', '', mam_book['isbn']) if mam_book['isbn'] and mam_book['isbn'].isdigit() else ''
    isbn = isbn if len(isbn) in [10, 13] else ''

    if isbn:
        if sess.get(f'https://libgen.rs/json.php?isbn={isbn}&fields=Title,Author', timeout=5).json():
            print(f'book is on libgen')
            return

    reduce_query_str = lambda query_str: ' '.join([x for x in query_str.split(' ') if len(x) > 1 and x.lower() not in stop_words])  # remove some words
    get_title_varients = lambda x: {re.sub(' *(?:\:.*|\(.*\))* *$', '', x), re.sub(' *(?:\(.*\))* *$', '', x), x}
    remove_except_alphanum = lambda x: re.sub('[^\w]', '', x)

    db_exact_match = False
    for author_idx, author in enumerate(authors[:3]):
        query_str = f'{title} {author}'
        query_str = reduce_query_str(query_str)
        # lg_results = libgen_search.search_lg(query_str, format='epub') if fiction else libgen_search.check_book_on_lg_nonfic(query_str)
        # search both fiction and nonfiction for the book
        lg_results = libgen_search.search_lg(query_str, format='epub') or libgen_search.check_book_on_lg_nonfic(query_str)
        if lg_results:
            print(f'book is on libgen {lg_results}')
            return

        # if we already know the book is on a book database don't bother checking again
        if db_exact_match:continue

        for db_title, db_author in query_book_databases(query_str):
            valid_titles = get_title_varients(title.lower())
            db_exact_match = bool(get_title_varients(db_title.lower()).intersection(valid_titles))
            if db_exact_match:
                # redo the search with the gr title
                # while we have the proper title we might as well try to fix the title's capitalization
                if db_title.lower() == title.lower() and not db_title == title and \
                        not (db_title.isupper() or db_title.islower()):
                    logger.info(f'changing from title "{title}" to "{db_title}"')
                    title = db_title
                # also try and fix the author cause why not
                if db_title != author and remove_except_alphanum(author) == remove_except_alphanum(db_author):
                    logger.info(f'changing from author "{author}" to "{db_author}"')
                    authors[author_idx] = db_author

                if db_title != title or db_author != author:
                    db_query_str = f'{db_title} {db_author}'
                    lg_results = libgen_search.search_lg(db_query_str, format='epub') if fiction else\
                        libgen_search.check_book_on_lg_nonfic(db_query_str)
                    if lg_results:
                        print(f'book is on libgen {lg_results}')
                        return
                break  # since we've got an exact match exit this loop

    if not db_exact_match:
        print(f'isnt on goodreads torrent id:{mam_book["id"]}')
        return

    get_dl_url = lambda x: \
        f'https://www.myanonamouse.net/tor/download.php/{x}'
    torrent_dl_url = get_dl_url(mam_book['dl'])
    torrent_dl_request = sess.get(torrent_dl_url)
    logger.info(f'adding "{title}" {mam_book["id"]} to transmission client')
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
        if i > 600 and not done_downloading:  # wait up to x seconds for ebook to download
            logger.info(f'torrent failed to download {t_files}')
            return

    if torrent.download_dir != client_download_dir:
        torrent.move_data(client_download_dir)  # make sure files are where they should be
    # check that downloads are completed and there's only 1 epub
    t_file_names = [x.name for x in t_files if x.name.endswith('.epub')]

    epub_paths = [os.path.join(download_dir, x) for x in t_file_names]
    if not epub_paths:
        logger.info('didnt download any epubs')
        return
    if len(epub_paths) > 1:
        logger.info(f'got too many epubs {epub_paths}')
        return

    epub_path = epub_paths[0]
    if not os.path.exists(epub_path):
        logger.info(f'cant find epub where its expected {epub_path}')
        return

    try:book_txt = get_book_text(epub_path)
    except Exception as e:
        logger.info(f'error while reading epub. bad file? {e}')
        return
    if book_txt.count(' ') < 10 and len(book_txt) < 1_000:
        logger.info(f'book too short? {book_txt}')
        return

    publisher = get_publisher(epub_path)
    publisher = publisher if publisher and len(publisher) < 100 else ''


    upload_url = upload_ebook_to_libgen(epub_path) if fiction else upload_ebook_to_libgen(epub_path, libgen_upload_url='https://library.bz/main/upload/')
    if not upload_url:
        logger.info(f'upload failed for "{title}" torrent id: ' + str(mam_book['id']))
    else:
        print(title, epub_path, upload_url)
        r = lg_edit_meta(edit_url=upload_url,
                         title=title, author=', '.join(authors), series=series, isbn=isbn, description=mam_book['description'],
                         publisher=publisher, asin=asin)
        logger.info('successful upload' + ('.' if r.status_code == 200 else '?') + f' "{title}" {mam_book["id"]} {r.status_code} {r.url}')


def get_books_from_mam(num_to_fetch=50, start_num=0):
    my_cookies = {
        'mam_id': mam_id}

    r = sess.get('https://www.myanonamouse.net/jsonLoad.php?snatch_summary', cookies=my_cookies, timeout=60)
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
                "sortType": 'dateAsc',
                "startNumber": str(start_num),
                "text": '@filetype epub',
                "minSeeders": 2,
                "endDate": date_str_end,  # only look at uploads 4d old to respect MAM rule 2.2
                "browse_lang": ['1'],  # english only
                "minSize": '10',
                "maxSize": str(40*1024),
                "unit": '1024',  # size restrictions limit libgen uploads. must be in range 10Kb - 200 Mb
                "srchIn": {
                    x: "true" for x in (searchin if type(searchin) == list else [searchin])
                }
            },
            "dlLink": "true",
            "isbn": "true",
            "description": "true"
        }

        r = sess.post('https://www.myanonamouse.net/tor/js/loadSearchJSONbasic.php', json=json_dict,
                      cookies=my_cookies, timeout=60)
        start_num += r.json()['perpage']
        # only accept torrents with less than 3 files to avoid collections
        mam_books += [x for x in r.json()['data'] if x['numfiles'] <= 2 and x['id'] not in upload_blacklist]
        if start_num >= r.json()['found']:
            print(f'no more MAM books to process')
            break
    return mam_books, mam_limit, start_num


mam_fiction_cats = [60, 102, 65, 109, 70, 64, 94, 62, 68, 63, 69, 112, 66, 67]
mam_nonfiction_cats = [71, 72, 90, 101, 107, 74, 76, 77, 115, 91, 78, 80, 92]
unwanted_cats = [61, 79]
html_to_bbcode_parser = HTML2BBCode()

transmission_username, transmission_password = 'username:password'.split(':')
transmission_ip = '127.0.0.1'
transmission_port = 9091  # 9091 is transmission's default port
lg_public_auth = 'Basic Z2VuZXNpczp1cGxvYWQ='  # not a secret. it's genesis:upload base64 encoded

trans_client = Client(host=transmission_ip, port=transmission_port, username=transmission_username, password=transmission_password)
client_download_dir = '/download/'  # path where torrent client will download the ebooks to
download_dir = '/download/'  # path that the computer running this script will find the downloaded ebooks at

mam_id = ''
resume_pagination_of_mam = True

limit = 50
sess = requests.Session()
stop_words = ['a', 'the', 'and', 'aka', 'book', 'novel']  # won't include these words in queries

if '__file__' in globals():
    pwd = os.path.split(__file__)[:-1]
else:
    pwd = []
blacklist_filepath = os.path.join(*pwd, 'mam_id_blacklist.txt')
upload_blacklist = []
if os.path.exists(blacklist_filepath):
    with open(blacklist_filepath, 'r') as f:
        upload_blacklist = [int(x.strip()) for x in f.readlines()]

mam_start_num_filepath = os.path.join(*pwd, 'mam_start_num.json')
mam_start_num = 0
if os.path.exists(blacklist_filepath) and resume_pagination_of_mam:
    with open(mam_start_num_filepath, 'r') as f:
        mam_start_num = json.load(f)


if __name__ == '__main__':
    while limit > 0:
        mam_books, mam_limit, mam_start_num = get_books_from_mam(num_to_fetch=limit, start_num=mam_start_num)
        limit = min(limit, mam_limit)
        logger.info(f'limit of {limit}')
        logger.info(f'start_num: {mam_start_num}')

        for mam_book in mam_books:
            if mam_book['id'] in upload_blacklist:
                print(f'{mam_book["title"]} in blacklist')
                continue
            else:
                with open(blacklist_filepath, 'a') as f:
                    f.write(str(mam_book['id']) + '\n')
                upload_blacklist.append(mam_book['id'])

            download_parse_metadata_and_upload(mam_book)

            if limit <= 0:
                print(f'reached upload limit')
                break
        if limit > 0:
            if resume_pagination_of_mam:
                with open(mam_start_num_filepath, 'w') as f:
                    f.write(json.dumps(mam_start_num))
