

import logging
import sys
import csv
import re
import argparse

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
def upload2db(sql, data,db) :
    conn = dbapi.connect(address=db['host'], port=db['port'], user=db['user'], password=db['pwd'], encrypt=True,
                         sslValidateCertificate=False)

    cursor = conn.cursor()

    if db['batchsize'] == 0 :
        print('Uploading: {}'.format(len(data)))
        cursor.executemany(sql, data)
    elif db['batchsize'] == 1 :
        print('Uploading each record separately: {}'.format(len(data)))
        for i,rec in enumerate(data) :
            if db['info_batchsize']> 0 and  i%db['info_batchsize'] == 0 :
                print('Uploaded: {}/{}'.format(i,len(data)))
            try :
                cursor.execute(sql, rec)
            except dbapi.DataError as de :
                logging.warning('Data Error: line {} ({})'.format(i,de))
                logging.warning('Record: {}'.format(rec))
    else:
        print('Uploading in batches: {} of batch size: {} (#{})'.format(len(data),db['batchsize'],int(len(data)/db['batchsize'])+1))
        for i in range(0,len(data),db['batchsize']) :
            print('Uploaded: {}/{} - Uploading: {}'.format(i,len(data),len(data[i:i+db['batchsize']])))
            cursor.executemany(sql, data[i:i + db['batchsize']])

    cursor.close()
    conn.close()

def execute_select(db,sql,params) :
    conn = dbapi.connect(address=db['host'], port=db['port'], user=db['user'], password=db['pwd'], encrypt=True,
                         sslValidateCertificate=False)
    cursor = conn.cursor()
    ret = cursor.execute(sql)
    print('SQL: {}'.format(sql))
    if ret :
        print('DB connected')
    else :
        print('DB connection failed!')
        return None
    rows = cursor.fetchall()
    if len(rows) > params['DISPLAY_ROWS'] :
        print('First {} rows: '.format(params['DISPLAY_ROWS']))
    print('Results:')
    for r in rows[:params['DISPLAY_ROWS']] :
        print(str(r)[1:-1])
    cursor.close()
    conn.close()
    return rows

def execute_sql(db,sql) :
    conn = dbapi.connect(address=db['host'], port=db['port'], user=db['user'], password=db['pwd'], encrypt=True,
                         sslValidateCertificate=False)

    print('SQL: {}'.format(sql))
    if conn :
        print('DB connected')
    else :
        print('DB connection failed!')
        return None

    cursor = conn.cursor()
    ret = cursor.execute(sql)
    if ret :
        print('Successful')
    cursor.close()
    conn.close()

def run_sql(db,sql,params) :

    sqlupload = True if re.match('INSERT\s+.+\(\s*\?\s*,',sql) or re.match('UPSERT\s+.+\(\s*\?\s*,',sql) else False
    sqlselect = True if re.match('SELECT\s+', sql) else False

    try:
        if sqlupload :
            records = read_csv(params['INPUT_CSV'])
            upload2db(sql=sql,data=records,db=db)
        elif sqlselect :
            data = execute_select(db=db,sql=sql,params=params)
            write_csv(params['OUTPUT_FILE'], sql, data)
        elif sql:
            execute_sql(db=db,sql=sql)
        else :
            conn = dbapi.connect(address=db['host'], port=db['port'], user=db['user'], password=db['pwd'], encrypt=True,
                                 sslValidateCertificate=False)
            if conn:
                print('DB connected')
            else:
                print('DB connection failed!')
                return None
    except dbapi.ProgrammingError as pe :
        logging.warning(pe)

def console(db,params) :
    print('Enter sql-statememt')
    while True :

        sql = input('> ')
        if re.match('quit',sql) :
            break
        run_sql(db,sql,params)

def main() :
    #
    # command line args
    #
    parser = argparse.ArgumentParser(description="Runs sql-statetments from commandline or interactively.")
    parser.add_argument('-i','--interactive', help = 'Opens console for interactive input', action="store_true")
    parser.add_argument('-s', '--sql', help='Runs sql-statement')
    args = parser.parse_args()

    # Logging
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    #
    # configuration
    #
    with open('config.yaml') as yamls:
        params = yaml.safe_load(yamls)

    #logging.info('Setting DB connection parameter.')
    db = {'host':params['HDB_HOST'],
          'user':params['HDB_USER'],
          'pwd':params['HDB_PWD'],
          'port':params['HDB_PORT'],
          'batchsize' : params['BATCHSIZE'],
          'info_batchsize' : params['INFO_BATCHSIZE']}


    if args.sql :
        run_sql(db,args.sql,params=params)

    if args.interactive :
        console(db,params=params)



if __name__ == '__main__':
    main()
