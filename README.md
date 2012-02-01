CheckMySQLTableStatus a script to monitor MySQL table status.

## Description

Executes just "show table status" queries for all schemas on the server. Parse the output.

Gives

* approximate row count,
* data size,
* index size, 
* free data size,
* last auto increment value

as performance data for tables.

## Usage

```
./checkMySQLProcesslist.sh -h
```

### Exit Status

* 0 for ok
* 1 for warning
* 2 for critical
* 3 for unknown
