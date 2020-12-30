# Data config
from psycopg2 import extensions
import strategy_default
import strategy_udf
import strategy_small_transactions

# How many different sweets in table
n_sweets = 50
# Amount of each sweet (value of column 'ilosc_pozostalych')
amount_each = 30
# Amount of rows in 'podobny_slodycz' table
amount_similiar = int(n_sweets)

# How many of each item in the request maximum
request_item_count = 2
# How many different items in the request
request_amount = 5

isolation_level = extensions.ISOLATION_LEVEL_READ_COMMITTED


test_time_seconds = 10

adversary_random_sleep_time = 0.5
adversary_bad_sleep_time = 3

#strategy_to_test = strategy_default
#strategy_to_test = strategy_udf
strategy_to_test = strategy_small_transactions
strategy_args = (20, 0, 0, test_time_seconds) 
#strategy_args = (10, 0, 0, test_time_seconds) # default
