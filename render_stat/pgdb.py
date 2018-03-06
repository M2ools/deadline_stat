import util
util.venv_to_syspath()
import psycopg2


def get_connection_tokens(json_file='pg.json'):
    return util.read_json('pg.json')


def init_connection(host, db, user, pwd):
    try:
        conn = psycopg2.connect(
            "host='%s' dbname='%s' user='%s' password='%s'" %
            (host, db, user, pwd)
        )
        return conn
    except psycopg2.OperationalError as e:
        print "Error, {}".format(e)


