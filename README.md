# BulkInsertTest

Command line program to test different insert methods.

## Syntax

```
InsertTester -t <thread num> -r <rows per thread> -c <connection string> -i <insert mode> |-b <batch size>| |-p user partitioned table|
```

| Parameter | Type | Required | Description |
| ---- | ----- | ----- | ------ |
| -t | Int | yes | Number of threads to spawn. Must be > 0. |
| -r | Long | yes | Rows to insert per thread. Must be > 0. |
| -c | String | yes | Connection string to the pre-created database (see [tsql.sql]()) |
| -i | Enum | yes | Insert mode. Execute ```InsertTester``` without parameters to see the supported options. |
| -b | Long | no | Batch size (makes sense only with bulk inserts). To see the default execute ```InsertTester``` without parameters |
| -p | bool | no | Load into the partitioned table. To see the default execute ```InsertTester``` without parameters |
