#!/usr/bin/env python3

# github.com/rafaelmdcarneiro 13-12-2017

import glob
import sys
import os.path
import pathlib
import psycopg2
from random import shuffle
from psycopg2 import sql

files = glob.glob('data/*/*', recursive=True)

connect_str = "dbname='pw' user='xxxxxxxxxx' host='xxxxxxxxxx' password='xxxxxxxxxx'"
conn = psycopg2.connect(connect_str)
cursor = conn.cursor()

def mark(c):
    sys.stdout.write(c)
    sys.stdout.flush()

shuffle(files)

for pwfile in files:
    print('{} > '.format(pwfile), end = '')

    if os.path.isdir(os.path.join('done', pwfile)):
        print('file is in done')
        continue

    n = 0
    b = 0
    with open(pwfile, 'r', encoding='utf-8', errors='surrogateescape') as f:
        try:
            for line in f:
                up = line.encode('utf-8', errors='surrogateescape').decode('utf8', 'replace').split(':')
                user = up[0]
                if len(up) != 2:
                    passw = ''
                else:
                    passw = ':'.join(up[1:])

                try:
                    cursor.execute('insert into passwords (password_username, password_password) values (%s, %s)', [user, passw])
                    n += 1
                    if n % 1000 == 0:
                        conn.commit()
                        mark('.')
                except psycopg2.IntegrityError as e:
                    conn.rollback()
                    b += 1
                    if b % 1000 == 0:
                        mark('!')
                except ValueError as e:
                    conn.rollback()
                    passw = '\\0'
                    mark('0')

        except UnicodeDecodeError as e:
            print(e)
            raise e

    print()
    pathlib.Path(os.path.join('done', pwfile)).mkdir(parents=True, exist_ok=True)

print('all done...')

