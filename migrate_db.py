from StringIO import StringIO
import pandas as pd
from IPython import embed
import psycopg2

# localhost_db_connect()
localhost_conn = psycopg2.connect("dbname='doris_postgre' user='doris' host='localhost' password='' port='5432'")

server_conn = psycopg2.connect("dbname='doris_postgre' user='doris' host='localhost' password='' port='5432'")

table = 'master'

if not os.path.isdir('/data/tmp/eod'):
	os.mkdir('/data/tmp/eod')

filename = '/data/tmp/eod/{0}.csv'.format(table)

localhost_cursor = localhost_conn.cursor()

with open(filename, 'w') as f:
	localhost_cursor.copy_to(f, table, sep='|', null='')

server_cursor = server_conn.cursor()
with open(filename, 'r') as f:
	server_cursor.copy_from(f, table, sep='|', null='')



