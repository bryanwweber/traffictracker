from __future__ import print_function

import json
import sqlite3
import time
import dateutil.parser
from datetime import datetime
import os
import pycurl
from io import BytesIO
from repo import repo_data


def main():
    # get credentials
    with open('access.cred', 'r') as file:
        cred = file.readlines()[0].strip()

    # create command
    curl_header = ['Authorization: token {0}'.format(
        cred), 'Accept: application/vnd.github.spiderman-preview']
    curl_command = 'curl -i -H "" -H ""'
    curl_url = 'https://api.github.com/repos/{0}/{1}/traffic/{{}}'.format(
        repo_data['user'], repo_data['repo'])
    curl_command = curl_command.format(cred)

    # create / open DB
    db = sqlite3.connect('traffic.db', detect_types=sqlite3.PARSE_DECLTYPES)
    c = db.cursor()

    # create schema
    try:
        c.execute('''CREATE TABLE traffic
                    (time timestamp UNIQUE, views INTEGER, unique_views INTEGER)
                ''')
    except sqlite3.OperationalError, e:
        if str(e) != "table traffic already exists":
            raise e

    try:
        c.execute('''CREATE TABLE clones
                    (time timestamp UNIQUE, count INTEGER, unique_count INTEGER)
                ''')
    except sqlite3.OperationalError, e:
        if str(e) != "table clones already exists":
            raise e

    tzinfo = datetime.now().tzinfo

    def __parse_row(row):
        try:
            date = dateutil.parser.parse(row['timestamp'], tzinfo)
        except:
            # try the old style timestamp
            date = datetime.fromtimestamp(row['timestamp'] / 1000., tzinfo)
        count = int(row['count'])
        unique = int(row['uniques'])
        return (date, count, unique)

    err_count = 0
    found = False
    while not found:
        try:
            view_data = BytesIO()
            # create curl objects
            py_views = pycurl.Curl()
            py_views.setopt(pycurl.URL, curl_url.format('views'))
            py_views.setopt(pycurl.HTTPHEADER, curl_header)
            # py_views.setopt(pycurl.HEADER, 1)
            py_views.setopt(pycurl.WRITEFUNCTION, view_data.write)

            clone_data = BytesIO()
            py_clones = pycurl.Curl()
            py_clones.setopt(pycurl.URL, curl_url.format('clones'))
            py_clones.setopt(pycurl.HTTPHEADER, curl_header)
            # py_clones.setopt(pycurl.HEADER, 1)
            py_clones.setopt(pycurl.WRITEFUNCTION, clone_data.write)

            # query view api
            py_views.perform()
            views = json.loads(view_data.getvalue())

            # parse into rows for DB
            view_rows = [__parse_row(row) for row in views['views']]
            c.executemany('INSERT OR REPLACE INTO traffic VALUES (?, ?, ?)',
                          view_rows)

            # query clone api
            py_clones.perform()
            clones = json.loads(clone_data.getvalue())

            # parse into rows for DB
            clone_rows = [__parse_row(row) for row in clones['clones']]
            c.executemany('INSERT OR REPLACE INTO clones VALUES (?, ?, ?)',
                          clone_rows)
            db.commit()

            found = True

        except KeyError as e:
            raise Exception(
                'Repo {}/{} not found!'.format(repo_data['user'], repo_data['repo']))

        except Exception as e:
            err_count += 1
            time.sleep(5 * 60)
            if err_count == 10:
                raise e


if __name__ == '__main__':
    main()
