Infinispan Server Integration Tests
========

This module contains the Infinispan Server Integration test suite. It uses `infinispan-server-testdriver` as a harness.

## Testsuite Categories

Tests are annotated with JUnit tags. The following tags are available:

* `Database`
* `Persistence`
* `Resilience`
* `Security`

The default is to run all tags, but this can be overridden by setting the `defaultJUnitGroups` system property, e.g.

`mvn -DdefaultExcludedJUnitGroups=Persistence ...`

## JDBC Tests

The JDBC tests can run against either standalone or containerized databases.
The default is to test against containerized versions of H2, MySQL and PostgreSQL.
You can specify a different set of databases by setting the system property `org.infinispan.test.database.types`, e.g.:

`mvn -Dorg.infinispan.test.database.types=h2,mysql,postgres,mariadb,oracle,db2,sql_server ...`

In order for the test to work, for each supplied database name `id` there must be a
`src/test/resources/database/id.properties` file which describes the database types and configuration.
You can also use the `org.infinispan.test.database.properties.path`system property to specify the path to your database
configuration

To add JDBC drivers to the servers in order to run tests against specific databases, you can use
`org.infinispan.test.database.jdbc.drivers` or `org.infinispan.test.database.jdbc.drivers.file` system property:

`mvn -Dorg.infinispan.test.database.jdbc.drivers=com.h2database:h2:1.4.199,com.oracle.database.jdbc:ojdbc11:jar:23.5.0.24.07 ...`

`mvn -Dorg.infinispan.test.database.jdbc.drivers.file=/opt/jdbc-drivers.txt`

You can also specify an external JDBC driver which is not find into maven repositories using
`org.infinispan.test.database.jdbc.drivers.external` system property, e.g.::
`mvn -Dorg.infinispan.test.database.jdbc.drivers.external="/opt/jconn4.jar,/opt/another-jdbc.driver.jar`

To pass in the address of external databases, use the `org.infinispan.server.test.database.id.address` system property,
replacing `id` with the name of your database:

`mvn -Dorg.infinispan.server.test.database.db2.address=10.1.2.3 ...`

## Additional rules

### LdapServerRule

This `@ClassRule` configures and manages an ApacheDS LDAP server using a supplied LDIF. The LDAP server is executed
embedded in the same JVM which is running the test.

### KeyCloakServerRule

This `@ClassRule` configures and manages a KeyCloak server with a supplied JSON export file. The KeyCloak server is
executed within a Docker container.