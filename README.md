CheckMySQLTableStatus a script to monitor MySQL table status.

## Description

Executes "show table status" queries for all schemas on the server. Parse the
output. Gives Nagios compatible warning, critical notifications and performance
data for selected values.

## Exit Status

* 0 for ok
* 1 for warning
* 2 for critical
* 3 for unknown

## Examples

```
./checkMySQLTableStatus.py -h
```

```
./checkMySQLTableStatus.py -H *** -u *** -p *** \
        -m rows,data_length,index_length,data_free,auto_increment \
        -w 100M,50G,50G,500M,2G
```

```
./checkMySQLTableStatus.py -u *** -p *** -w 10M,10G -c 100M
```

```
./checkMySQLTableStatus.py -m auto_increment -w 2G
```

## Source

github.com/tart/CheckMySQLTableStatus
