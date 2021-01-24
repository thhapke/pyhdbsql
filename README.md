# pyhdbsql

Command line app that connects to a HANA database and call an sql statement. The credentials of the database are stored in the config.yaml.

The outcome of a ```SELECT```- statement can be stored in an output-file given in the config-file. 

For uploading the data can be read from a csv-file of a path given in the config-file.

There are 2 ways to interact

* sql-statement passed as commandline option
* interactive console

Example 
```
Enter sql-statememt
> SELECT * FROM TESTTABLE
SQL: SELECT * FROM TESTTABLE
DB connected
Results:
3, 'Anna', 3
4, 'Berta', 2
5, 'Celine', 8
6, 'Dora', 9
7, 'EMILE', 9
10, 'Fanny', 9
11, 'Ginger', 3
> INSERT INTO TESTTABLE VALUES (12,'Hermione',2)
SQL: INSERT INTO TESTTABLE VALUES (12,'Hermione',2)
DB connected
Successful
> 


```


## Interactive

Sends entered SQL statement to database and returns result. 'quit' ends program. 

## Command line options

```
usage: pyhdbsql [-h] [-i] [-s SQL]

Runs sql-statetments from commandline or interactively.

optional arguments:
  -h, --help         show this help message and exit
  -i, --interactive  Opens console for interactive input
  -s SQL, --sql SQL  Runs sql-statement
```

## Example
```pyhdbsql -s "<sql-statement>"```  or explicitly

```pyhdbsql -s "SELECT * FROM TESTTABLE;"```

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


