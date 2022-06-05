import re
import requests
import json
import os
import libtorrent
import libgen_search


def get_multiplier(x):
    return 1024 ** ({
        "Kb": -1,
        "Mb": 0,
        "Gb": 1,
        "Tb": 2,
        "KiB": -1,
        "MiB": 0,
        "GiB": 1,
        "TiB": 2,
    }[x] + 2)


def convert_filesize_to_int(file_size):
    matches = re.match("([\d,\.]{1,}) (\w+)", file_size)
    return round(float(matches[1].replace(",","")) * get_multiplier(matches[2]), 3)


def search_for_torrent(title, author, torrent_filename, torrent_dl_url):
    if not os.path.exists(torrent_filename):
        torrent_dl_request = sess.get(torrent_dl_url)
        with open(torrent_filename, 'wb') as f:
            f.write(torrent_dl_request.content)

    t = libtorrent.torrent_info(torrent_filename)
    if not t.is_valid():
        print(f'torrent file {torrent_filename} isnt valid')
        return

    filesize = t.total_size()
    filename = t.files().name()
    file_ext = filename[::-1].split('.')[0][::-1]

    lg_results = libgen_search.search_lg(f'{title}')
    if not lg_results:return

    # further filter the results
    get_lg_api_url = lambda x: f'https://libgen.lc/json.php?object=f&md5={x}&fields=*'

    for lg_result in lg_results:
        if lg_result['Extension'] == file_ext:
            # if the filesize is somewhat close to what it should be
            if convert_filesize_to_int(lg_result['Size'])/filesize > 0.95:

                download_links = libgen_search.resolve_download_links(lg_result)
                if 'GET' not in download_links:continue

                # check filesize before downloading
                # try using libgen.lc's api to verify filesize
                try:
                    md5_hash = re.search(r'(?<!\w)[0-9a-f]{32}(?!\w)', download_links['GET']).group()
                    r_filesize = int(list(sess.get(get_lg_api_url(md5_hash)).json().values())[0]['filesize'])
                    if r_filesize != filesize:
                        continue
                except:pass

                r = sess.get(download_links['GET'])
                if len(r.content) == filesize:
                    print(f'found matching result for {title}')
                    ebook_filepath = os.path.join('ebooks', filename)
                    with open(ebook_filepath, 'wb') as f:
                        f.write(r.content)
                    return True


sess = requests.Session()
if not os.path.isdir('torrents'):
    os.mkdir('torrents')
if not os.path.isdir('ebooks'):
    os.mkdir('ebooks')

if '__file__' in globals():
    pwd = os.path.split(__file__)[:-1]
else:
    pwd = []
blacklist_filepath = os.path.join(*pwd, 'downloaded_tor_ids.txt')

if __name__ == '__main__':
    searchin = ['fileTypes']
    my_cookies = {
        'mam_id': ''}
    start_num = 0
    good_bad = [0, 0]

    # load blacklist
    with open(os.path.join('.', "downloaded_tor_ids.txt"), "r") as tor_ids_file:
        already_snatched = tor_ids_file.read().splitlines()

    for start_num in range(0, 100, 100):
        print(f'startnum {start_num} good_bad:{good_bad}')
        json_dict = {
            "tor": {
                "main_cat": ["14"],  # limit query to ebooks
                # "cat": ['68'],
                # "browseStart": "True",
                # "sortType": "seedersAsc",
                "searchIn": "allReseed",
                "sortType": 'dateAsc',
                "startNumber": str(start_num),
                "text": '"epub mobi azw3 pdf"/1',
                "srchIn": {
                    x: "true" for x in (searchin if type(searchin) == list else [searchin])
                }
            },
            "dlLink": "true",
        }
        r = sess.post('https://www.myanonamouse.net/tor/js/loadSearchJSONbasic.php', json=json_dict,
                                 cookies=my_cookies, timeout=20)
        if start_num > r.json()['found']:
            print('iterated through all mam torrents')
            break

        get_dl_url = lambda x: \
            f'https://www.myanonamouse.net/tor/download.php/{x}'


        for mam_torrent in r.json()['data']:
            if str(mam_torrent["id"]) in already_snatched:continue
            if mam_torrent['numfiles'] == 1 and mam_torrent['filetype'] in "epub mobi azw3 pdf".split(' '):
                title = mam_torrent['title']
                if type(title) == int:continue
                title = title.split(':')[0]  # remove subtitle from title string
                try:
                    for author in json.loads(mam_torrent['author_info']).values():break
                except:continue
                torrent_dl_url = get_dl_url(mam_torrent['dl'])
                torrent_filename = re.sub('[^\w ]', '', f'{title[:30]} - {author[:10]} {mam_torrent["id"]}') + '.torrent'
                torrent_filename = os.path.join('torrents', torrent_filename)
                # avoid repeating items
                if os.path.exists(torrent_filename):continue
                try:found = search_for_torrent(title, author, torrent_filename, torrent_dl_url)
                except Exception as e:
                    print(f'error {e}')
                    found = None
                if found:
                    good_bad[0] += 1
                else:
                    with open(blacklist_filepath, 'a') as f:f.write(f"{mam_torrent['id']}\n")
                    if os.path.exists(torrent_filename):
                        os.remove(torrent_filename)
                    good_bad[1] += 1
    print(good_bad)

