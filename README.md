# Transaction Isolation Levels in PostgreSQL, MySQL, SQL Server, and Oracle with Examples in Go

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
- snapshot
- serializable