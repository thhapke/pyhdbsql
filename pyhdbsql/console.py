

import logging
from argparse import ArgumentParser
import sys
import csv
import re

import yaml
from hdbcli import dbapi


def write_csv(filename,sql,records) :
    with open(filename,'w') as file :
        writer = csv.writer(file,delimiter = ',')
        file.write('# {}\n'.format(sql))
        for r in records :
            writer.writerow(r)
    file.close()

def read_csv(filename) :
    records = list()
    with open(filename,mode='r',newline='\n') as csvfile :
        csvreader = csv.reader(csvfile,delimiter = ',')
        for line in csvreader:
            records.append(line)
    return records


def main() :
    ###
    # configuration
    ###
    with open('config.yaml') as yamls:
        params = yaml.safe_load(yamls)

    sql = ' '.join(sys.argv[1:])

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

    dataupload = True if re.match('INSERT\s+.+\(\s*\?\s*,',sql) or re.match('UPSERT\s+.+\(\s*\?\s*,',sql) else False

    if dataupload :
        records = read_csv(params['INPUT_CSV'])
        cursor = conn.cursor()
        ret = cursor.executemany(sql,records)
        logging.info('Data uploaded: {}'.format(ret))
        cursor.close()
        conn.close()

    elif sql :
        cursor = conn.cursor()
        ret = cursor.execute(sql)
        logging.info('SQL: {}'.format(sql))
        logging.info('DB response: {}'.format(ret))
        rows = cursor.fetchall()
        write_csv(params['OUTPUT_FILE'],sql,rows)
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()




