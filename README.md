# mysql2postgre
You can convert your MySQL tables (with datas) to your PostgreSQL server.

Example config file: (m2p.yml)

```bash
mysql_username: "root"
mysql_host: "1.1.1.1"
mysql_password: "123456"
mysql_port: 3306
mysql_database: "test"
mysql_table: "news"

postgre_username: "root"
postgre_host: "1.1.1.1"
postgre_password: "123456"
postgre_port: 5432
postgre_database: "test"
postgre_table: "news"

limit_copy_per_task: 50000 
```

