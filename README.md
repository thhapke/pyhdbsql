# pyhdbsql

Command line app that connects to a HANA database and call an sql statement. The credentials of the database are stored in the config.yaml.

The outcome of a ```SELECT```- statement can be stored in an output-file given in the config-file. 

For uploading the data can be read from a csv-file of a path given in the config-file.

## Example
```pyhdbsql <sql-statement>```  or explicitly

```pyhdbsql SELECT * FROM TESTTABLE;```

## config.yaml

```
###### HANA DB
HDB_HOST : db.com
HDB_USER : 'dbuser'
HDB_PWD : 'pwd123!'
HDB_PORT : 443

BATCHSIZE : 1  # uinteger, 0: no batches, 1: cursor.execute(), >1:cursor.executemany()
INFO_BATCHSIZE : 10000 # logging info when this number of records have been uploaded. Only used for BATCH_SIZE = 1, value 0: No info


### output file
OUTPUT_FILE : ./sqlout.csv

### input file for executemany
INPUT_CSV : ./records.csv

```


