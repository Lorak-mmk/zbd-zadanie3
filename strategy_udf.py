import prepare_tables
import util
from multiprocessing import Process, Barrier, Array
import time
import uuid
import config

def prepare_functions(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE OR REPLACE FUNCTION add_item_or_similiar_to_pack(pack_id INTEGER, item VARCHAR, amount INTEGER)
        RETURNS BOOLEAN
        VOLATILE PARALLEL UNSAFE
        LANGUAGE plpgsql
        AS
        $$
        DECLARE
            n INTEGER;
        BEGIN
            BEGIN
                SELECT ilosc_pozostalych INTO STRICT n 
                FROM slodycz_w_magazynie 
                WHERE nazwa = item AND ilosc_pozostalych >= amount 
                FOR UPDATE;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        BEGIN
                            SELECT podobny.slodycz_b INTO STRICT item
                            FROM podobny_slodycz AS podobny
                            LEFT JOIN slodycz_w_magazynie AS slodycz
                            ON podobny.slodycz_b = slodycz.nazwa
                            WHERE podobny.slodycz_a = item AND slodycz.ilosc_pozostalych >= amount
                            ORDER BY podobienstwo DESC 
                            LIMIT 1
                            FOR UPDATE;
                            EXCEPTION
                                WHEN NO_DATA_FOUND THEN
                                    RETURN FALSE;
                        END;
            END;
            UPDATE slodycz_w_magazynie SET ilosc_pozostalych = ilosc_pozostalych - amount WHERE nazwa = item;
            INSERT INTO slodycz_w_paczce (id_paczki, slodycz, ilosc) VALUES (pack_id, item, amount);
            RETURN TRUE;
        END
        $$;
        ''')

def default_transaction(cursor, request, do_sleep=False):
    cursor.execute('INSERT INTO paczka (kraj, name) VALUES (%s, %s) RETURNING id', (str(uuid.uuid4()), str(uuid.uuid4())))
    pack_id = cursor.fetchone()[0]
    for item, amount in request:
        cursor.execute('SELECT add_item_or_similiar_to_pack(%s, %s, %s)', (pack_id, item, amount))
        if do_sleep:
            time.sleep(random.uniform(0, config.adversary_random_sleep_time))
        result = cursor.fetchone()[0]
        if not result:
            raise Exception('Transaction failed (Error in UDF)')
    return

def normal_elf(barrier, results, errors, i):
    conn = util.get_connection('strategy_udf')
    conn.set_isolation_level( config.isolation_level )
    cursor = conn.cursor()
    ids = util.get_ids(cursor)
    conn.commit()
    barrier.wait()
    
    while True:
        request = util.generate_request(ids, config.request_amount, config.request_item_count)
        try:
            default_transaction(cursor, request)
            conn.commit()
            results[i] += 1
        except Exception as e:
            errors[i] += 1
            conn.rollback()
            
def adversary_random(barrier, results, errors, i):
    conn = util.get_connection('strategy_udf')
    conn.set_isolation_level( config.isolation_level )
    cursor = conn.cursor()
    ids = util.get_ids(cursor)
    conn.commit()
    barrier.wait()
    
    while True:
        request = util.generate_request(ids, config.request_amount, config.request_item_count)
        try:
            default_transaction(cursor, request, do_sleep=True)
            conn.commit()
        except Exception as e:
            errors[i] += 1
            conn.rollback()

def adversary_bad(barrier, results, errors, i):
    conn = util.get_connection('strategy_udf')
    conn.set_isolation_level( config.isolation_level )
    cursor = conn.cursor()
    ids = util.get_ids(cursor)
    conn.commit()
    barrier.wait()
    
    while True:
        request = util.generate_request(ids, config.request_amount, config.request_item_count)
        try:
            default_transaction(cursor, request)
            time.sleep(random.uniform(0, config.adversary_random_sleep_time))
            conn.commit()
        except Exception as e:
            errors[i] += 1
            conn.rollback()
    


def run(n_elfs, n_adversary_random, n_adversary_bad, time_seconds):
    util.create_database('strategy_udf')
    conn = util.get_connection('strategy_udf')
    cur = conn.cursor()
    print('Creating & populating tables & functions')
    prepare_tables.prepare_default_tables(conn)
    prepare_functions(conn)
    prepare_tables.populate_tables(cur, config.n_sweets, config.amount_each, config.amount_similiar)
    conn.commit()
    util.print_intro('udf', cur)
    
    n_all = n_elfs + n_adversary_random + n_adversary_bad
    b = Barrier(n_all)
    a = Array('i', [0] * (n_all))
    e = Array('i', [0] * (n_all))
    print('Starting test')
    elfs = [Process(target=normal_elf, args=(b, a, e, i)) for i in range(n_elfs)]
    elfs += [Process(target=adversary_random, args=(b, a, e, i + len(elfs))) for i in range(n_adversary_random)]
    elfs += [Process(target=adversary_bad, args=(b, a, e, i + len(elfs))) for i in range(n_adversary_bad)]
    for elf in elfs:
        elf.start()
    time.sleep(time_seconds)
    print('Terminating elfs')
    for elf in elfs:
        elf.terminate()
    util.print_report(a, e, n_elfs, n_all - n_elfs, time_seconds, cur)
    
    
