import psycopg2
from multiprocessing import Process
import util

COUNT = 10000
PROC_COUNT = 3

def test(n):
    conn = util.get_connection('test')
    conn.set_isolation_level( 0 )
    cursor = conn.cursor()
    for _ in range(n):
        cursor.execute('UPDATE test SET v = v + 1 WHERE k = 0')
    
    
    
util.create_database('test')
conn = util.get_connection('test')
cursor = conn.cursor()
cursor.execute('CREATE TABLE test(k int, v int)')
cursor.execute('INSERT INTO test (k, v) VALUES (0, 0)')
conn.commit()
proc = [Process(target=test, args=(COUNT,)) for _ in range(PROC_COUNT)]
for p in proc:
    p.start()

for p in proc:
    p.join()

cursor.execute('SELECT v FROM test WHERE k = 0')
print(f'Expected: {COUNT * PROC_COUNT}, real: {cursor.fetchone()[0]}')
