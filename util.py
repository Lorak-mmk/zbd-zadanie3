import psycopg2
from psycopg2 import extensions
import config
import random

def create_database(dbname):
    con = psycopg2.connect(host='127.0.0.1', user='postgres', password='docker', dbname='')
    con.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS {dbname}');
    cur.execute(f'CREATE DATABASE {dbname}')
    con.commit()

def get_connection(dbname):
    conn = psycopg2.connect(host='127.0.0.1', user='postgres', password='docker', dbname=dbname)
    return conn

def get_ids(cursor):
    cursor.execute('SELECT nazwa FROM slodycz_w_magazynie')
    return [row[0] for row in cursor.fetchall()]

def generate_request(ids, n, m):
    return [(name, random.randint(0, m)) for name in random.sample(ids, n)]
