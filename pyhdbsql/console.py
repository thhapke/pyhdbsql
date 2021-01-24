

import logging
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

### DB routines
def upload2db(sql, data,db,batchsize = 0,infobatch = 0  ) :
    conn = dbapi.connect(address=db['host'], port=db['port'], user=db['user'], password=db['pwd'], encrypt=True,
                         sslValidateCertificate=False)

    cursor = conn.cursor()

    if batchsize == 0 :
        logging.debug('Uploading: {}'.format(len(data)))
        cursor.executemany(sql, data)
    elif batchsize == 1 :
        logging.debug('Uploading each record separately: {}'.format(len(data)))
        for i,rec in enumerate(data) :
            if infobatch> 0 and  i%infobatch == 0 :
                logging.debug('Uploaded: {}/{}'.format(i,len(data)))
            try :
                cursor.execute(sql, rec)
            except dbapi.DataError as de :
                logging.warning('Data Error: line {} ({})'.format(i,de))
                logging.warning('Record: {}'.format(rec))
    else:
        logging.debug('Uploading in batches: {} of batch size: {} (#{})'.format(len(data),batchsize,int(len(data)/batchsize)+1))
        for i in range(0,len(data),batchsize) :
            logging.debug('Uploaded: {}/{} - Uploading: {}'.format(i,len(data),len(data[i:i+batchsize])))
            cursor.executemany(sql, data[i:i + batchsize])

    cursor.close()
    conn.close()

def execute_select(db,sql) :
    conn = dbapi.connect(address=db['host'], port=db['port'], user=db['user'], password=db['pwd'], encrypt=True,
                         sslValidateCertificate=False)
    cursor = conn.cursor()
    ret = cursor.execute(sql)
    logging.info('SQL: {}'.format(sql))
    logging.info('DB response: {}'.format(ret))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def execute_sql(db,sql) :
    conn = dbapi.connect(address=db['host'], port=db['port'], user=db['user'], password=db['pwd'], encrypt=True,
                         sslValidateCertificate=False)
    cursor = conn.cursor()
    ret = cursor.execute(sql)
    logging.info('SQL: {}'.format(sql))
    logging.info('DB response: {}'.format(ret))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

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

    sqlupload = True if re.match('INSERT\s+.+\(\s*\?\s*,',sql) or re.match('UPSERT\s+.+\(\s*\?\s*,',sql) else False
    sqlselect = True if re.match('SELECT\s+', sql) else False

    if sqlupload :
        records = read_csv(params['INPUT_CSV'])
        upload2db(sql=sql,data=records,db=db,batchsize=params['BATCHSIZE'],infobatch=params['INFO_BATCHSIZE'])

    elif sqlselect :
        data = execute_select(db=db,sql=sql)
        write_csv(params['OUTPUT_FILE'], sql, data)

    else:
        execute_sql(db=db,sql=sql)

if __name__ == '__main__':
    main()




