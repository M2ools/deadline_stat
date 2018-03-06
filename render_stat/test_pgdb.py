import pgdb

tokens = pgdb.get_connection_tokens()
conn = pgdb.init_connection(**tokens)
if conn:
    curr = conn.cursor()
    curr.execute('SELECT * FROM test')
    data = curr.fetchall()
    curr.close()
    conn.close()

    for d in data:
        print d