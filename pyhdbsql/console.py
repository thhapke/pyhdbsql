
from hdbcli import dbapi
import logging
import yaml
from argparse import ArgumentParser

def main() :
    ###
    # configuration
    ###
    with open('config.yaml') as yamls:
        params = yaml.safe_load(yamls)

    parser = ArgumentParser(description='hdb sql console')
    parser.add_argument('--sql', '-s', help='sql-statement in double quotes')
    args = parser.parse_args()
    sql = args.sql

    # Logging
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    logging.info('Setting DB connection parameter.')
    db = {'host':params['HDB_HOST'],
          'user':params['HDB_USER'],
          'pwd':params['HDB_PWD'],
          'port':params['HDB_PORT']}

    logging.info('HDB connection parameter read')

    conn = dbapi.connect(address=db['host'], port=db['port'], user=db['user'], password=db['pwd'], encrypt=True,
                         sslValidateCertificate=False)

    logging.info('Connection to db: {}'.format(conn))

    if sql :
        cursor = conn.cursor()
        ret = cursor.execute(sql)
        logging.info('SQL: {}'.format(sql))
        logging.info('DB response: {}'.format(ret))
        rows = cursor.fetchall()
        for r in rows:
            logging.info(r)
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()




