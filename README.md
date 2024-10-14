# Уровни изоляции транзакций в PostgreSQL, MySQL, SQL Server, Oracle с примерами на Go

*************************************************************************

1) go build .
2) ./transaction_isolation init_db [database]
  
[database] : 
- postgres
- mysql
- sqlserver
- oracle
3) ./transaction_isolation [isolation_level] [database]

[isolation_level] :
- read_uncommitted
- read_committed
- repeatable_read
- snapshot_isolation
- serializable