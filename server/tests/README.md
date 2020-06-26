Infinispan Server Integration Tests
========

This module contains the Infinispan Server Integration test suite. It uses `infinispan-server-testdriver` as a harness.

## Testsuite Categories

Tests are annotated with JUnit's `@Category` annotation. The following categories are available:

* `Java11`
* `Persistence`
* `Profiling`
* `Resilience`
* `Security`
* `Smoke`
* `Stress`
* `Unstable`

The default is to run all categories, except `[Unstable, Profiling, Stress]`, but this can be overridden by setting the `defaultJUnitGroups` system property, e.g.

`mvn -DdefaultExcludedJUnitGroups=Persistence ...`

## JDBC Tests

The JDBC tests can run against either standalone or containerized databases. 
The default is to test against containerized versions of H2, MySQL and PostgreSQL. 
You can specify a different set of databases by setting the system property `org.infinispan.test.server.jdbc.databases`, e.g.:

`mvn -Dorg.infinispan.test.server.jdbc.databases=h2,mysql,postgres,mariadb,oracle,db2,sql_server ...`

In order for the test to work, for each supplied database name `id` there must be a `src/test/resources/database/id.properties` file which describes the database types and configuration.

To add JDBC drivers to the servers in order to run tests against specific databases, use the `org.infinispan.test.server.extension.libs` system property:

`mvn -Dorg.infinispan.test.server.extension.libs=com.h2database:h2:1.4.199,com.oracle.jdbc:ojdbc8:jar:18.3.0.0 ...`

To pass in the address of external databases, use the `org.infinispan.server.test.database.id.address` system property, replacing `id` with the name of your database:

`mvn -Dorg.infinispan.server.test.database.db2.address=10.1.2.3 ...`

## Additional rules

### LdapServerRule

This `@ClassRule` configures and manages an ApacheDS LDAP server using a supplied LDIF. The LDAP server is executed embedded in the same JVM which is running the test. 

### KeyCloakServerRule

This `@ClassRule` configures and manages a KeyCloak server with a supplied JSON export file. The KeyCloak server is executed within a Docker container.