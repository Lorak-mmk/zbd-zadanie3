import uuid
import random
from psycopg2 import extras


def populate_tables(cursor, n_sweets, amount_each, amount_similiar):
    data = [(str(uuid.uuid4()), amount_each) for _ in range(n_sweets)]
    extras.execute_values(cursor, 'INSERT INTO slodycz_w_magazynie (nazwa, ilosc_pozostalych) VALUES %s', data)
    pairs = set()
    for _ in range(amount_similiar):
        (a, b) = random.randrange(len(data)), random.randrange(len(data))
        while a == b or (a, b) in pairs or (b, a) in pairs:
            (a, b) = random.randrange(len(data)), random.randrange(len(data))
        pairs.add((a, b))
    similarity_data = [(data[a][0], data[b][0], random.uniform(0, 100)) for (a, b) in pairs]
    similarity_data2 = [(x[1], x[0], x[2]) for x in similarity_data]
    extras.execute_values(cursor, 'INSERT INTO podobny_slodycz (slodycz_a, slodycz_b, podobienstwo) VALUES %s', similarity_data)
    extras.execute_values(cursor, 'INSERT INTO podobny_slodycz (slodycz_a, slodycz_b, podobienstwo) VALUES %s', similarity_data2)
    return [x[0] for x in data]


def prepare_default_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""DROP TABLE IF EXISTS slodycz_w_magazynie""")
    cursor.execute("""DROP TABLE IF EXISTS paczka""")
    cursor.execute("""DROP TABLE IF EXISTS slodycz_w_paczce""")
    cursor.execute("""DROP TABLE IF EXISTS podobny_slodycz""")
    
    cursor.execute("""CREATE TABLE slodycz_w_magazynie (
        nazwa varchar PRIMARY KEY, 
        ilosc_pozostalych int,
        CHECK(ilosc_pozostalych >= 0)
    )""")
    cursor.execute("""CREATE TABLE paczka (
        id serial PRIMARY KEY, 
        kraj varchar, 
        name varchar
    )""")
    cursor.execute("""CREATE TABLE slodycz_w_paczce (
        id_paczki int,
        slodycz varchar,
        ilosc int
    )""")
    cursor.execute("""CREATE TABLE podobny_slodycz (
        slodycz_a varchar,
        slodycz_b varchar, 
        podobienstwo double precision,
        PRIMARY KEY(slodycz_a, slodycz_b)
    )""")

    conn.commit()
