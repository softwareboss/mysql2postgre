# mysql2postgre
You can convert your MySQL tables (with datas) to your PostgreSQL server.

Example config file: (m2p.yml)

```bash
mysqlUsername: "root"
mysqlHost: "1.1.1.1"
mysqlPassword: "123456"
mysqlPort: 3306
mysqlDatabase: "test"
mysqlTable: "news"

postgreUsername: "root"
postgreHost: "1.1.1.1"
postgrePassword: "123456"
postgrePort: 5432
postgreDatabase: "test"
postgreTable: "news"

limitCopyPerTask: 50000 
```

