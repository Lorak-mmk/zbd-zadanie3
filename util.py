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


def get_sums(cursor):
    cursor.execute('SELECT SUM(ilosc_pozostalych) FROM slodycz_w_magazynie')
    magazine = cursor.fetchall()[0][0]
    cursor.execute('SELECT SUM(ilosc) FROM slodycz_w_paczce')
    packs = cursor.fetchall()[0][0]
    if magazine is None:
        magazine = 0
    if packs is None:
        packs = 0
    return magazine, packs


def print_report(success_array, error_array, n_elfs, n_adversaries, time_seconds, cursor):
    n_all = n_elfs + n_adversaries
    result_success = round((sum(success_array) / n_elfs) / time_seconds, 2)
    result_error = round((sum(error_array) / n_all) / time_seconds, 2)
    magazine, packs = get_sums(cursor)
    print(f'Successes: {result_success} tr/s per normal elf (sum: {sum(success_array)} successes)')
    print(f'Errors: {result_error} tr/s per any elf (sum: {sum(error_array)} errors)')
    print(f'In magazine: {magazine}, in packs: {packs}, sum: {magazine + packs}')
    
def print_intro(name, cursor):
    print(f'Testing {name} strategy')
    magazine, packs = get_sums(cursor)
    print(f'In magazine: {magazine}, in packs: {packs}, sum: {magazine + packs}')
