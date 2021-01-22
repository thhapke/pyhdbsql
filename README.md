# pyhdbsql

Command line app that connects to a HANA database and call an sql statement. The credentials of the database are stored in the config.yaml.

## Example
```hyhdbsql --sql "<sql-statement>"``` like ``
`hyhdbsql --sql "SELECT * FROM TESTTABLE;"```

## config.yaml

```
###### HANA DB
HDB_HOST : db.com
HDB_USER : 'dbuser'
HDB_PWD : 'pwd123!'
HDB_PORT : 443

```


