### How to test?

By default, the tests will run against H2 and SQLITE with the command

`mvn clean test`

You can override the databases using comma separated list of databases with JDBC connections parameters, for example:

##### Run against Mysql

`mvn clean test -Dorg.infinispan.test.sqlstore.database=mysql -Dorg.infinispan.test.sqlstore.jdbc.url=jdbc:mysql://host.mysqlserver:3306/dbname -Dorg.infinispan.test.sqlstore.jdbc.username=username -Dorg.infinispan.test.sqlstore.jdbc.password=password`

##### Run against Mysql and PostgreSQL

`mvn clean test -Dorg.infinispan.test.sqlstore.database=mysql,postgres -Dorg.infinispan.test.sqlstore.jdbc.url=jdbc:mysql://host.mysqlserver:3306/dbname,jdbc:postgresql://postgresserver:5432/dbname -Dorg.infinispan.test.sqlstore.jdbc.username=username-mysql,username-postgres -Dorg.infinispan.test.sqlstore.jdbc.password=password-mysql,password-postgres`