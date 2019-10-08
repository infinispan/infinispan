# Infinispan Server Testsuite

The Server test suite is built around the following tools:

* JUnit 4.x
* Testcontainers 1.11.x

The general aim of the testsuite is to reduce server churn by reusing servers between tests that can share the same
configuration. Tests should therefore be grouped under suites which take care of configuring, starting and stopping the 
servers.

## Naming and layout conventions

* Suites and individual tests that cannot be run as part of a suite must have names ending with `Test` or `IT`.
* Tests which are part of suites **must not** have names ending with `Test` or `IT`. They can still be executed individually either from the IDE or from the command-line using Surefire's `-Dtest=TestName` or `-Dit.test=ITName`.
* Each suite together with all of its tests must be placed in a distinct directory.
* Configuration files are modularized using XInclude so that sections can be reused across multiple configurations without using XSLT trickery.

## Server run modes (aka Drivers)

The testsuite can run Infinispan Servers in different modes:

* **EMBEDDED**: all servers are run in the same JVM as the test
* **CONTAINER**: a Docker image is created on the fly

These are all handled by a specific driver which takes care of:

* setting up the server layout
* creating a CA and server and client certificates and assembling them into keystores/truststores
* creating user / role property files for authentication tests

The driver used by tests can be chosen by setting the system property `org.infinispan.test.server.driver` to one of the above names.
By default, the **EMBEDDED** driver will be used. When running the testsuite through Maven, you can force the use of the **CONTAINER** driver by using the `container` profile:

```
mvn -Pcontainer verify
```

## JUnit Rules

The testsuite comes with a number of JUnit rules which aid in the setup of server clusters.

### InfinispanServerRule

This `@ClassRule` must be used in both suites and individual tests. It specifies the configuration used by the servers and the number of nodes.
It creates a server directory layout for each node under `/tmp/infinispanTempFiles/fully.qualified.test.ClassName`. It then starts all
nodes and waits for the cluster to form. After all test methods / classes in the suite have been executed, it stops all of the servers.
When used in suites, it needs to be initialized in the suite class:

```java
@RunWith(Suite.class)
@Suite.SuiteClasses({
   A.class,
   B.class
})
public class LotsOfTests {

   @ClassRule
   public static final InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerTestConfiguration("config.xml").numServers(2));
}
```

and referenced in the test as follows:

```java
public class A {

   @ClassRule
   public static final InfinispanServerRule SERVERS = LotsOfTests.SERVERS;
}
```

### InfinispanServerTestMethodRule

This `@Rule` must be used in the actual tests. Each test method should use this instance to:

* obtain a client (Hot Rod, REST, Memcached) correctly configured against the running servers
* create a dedicated cache with either a default or supplied configuration named according to the test class/method name
* correctly clean up client resources after a test is completed 

```java
public class A {
   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);
}
```

### LdapServerRule

This `@ClassRule` configures and manages an ApacheDS LDAP server using a supplied LDIF. The LDAP server is executed embedded in the same JVM which is running the test. 

### KeyCloakServerRule

This `@ClassRule` configures and manages a KeyCloak server with a supplied JSON export file. The KeyCloak server is executed within a Docker container.

## Elasticity / Resilience tests

You can pause/resume/start/stop nodes during the lifecycle of a test. This is only possible when running with the CONTAINER driver.
From a test, invoke:

```java
SERVERS.getServerDriver().pause(0); // Sends a SIGSTOP to the specified server node
// ...
SERVERS.getServerDriver().resume(0); // Sends a SIGCONT to the specified server node
```

to pause a node and subsequently resume it.

## Testsuite Categories

Tests are annotated with JUnit's `@Category` annotation. The following categories are available:

* `org.infinispan.server.test.category.Persistence`
* `org.infinispan.server.test.category.Resilience`
* `org.infinispan.server.test.category.Security`

The default is to run all categories, but this can be overridden by setting the `defaultJUnitGroups` system property, e.g.

`mvn -DdefaultExcludedJUnitGroups=org.infinispan.server.test.category.Persistence ...`

## Testsuite Properties

The following is a list of properties which affect the build:

* `org.infinispan.test.server.baseImageName` the base image to use for the server. Defaults to `jboss/base-jdk:11`.
* `org.infinispan.test.server.driver`  the driver to use, `EMBEDDED` or `CONTAINER`. Defaults to the `EMBEDDED` driver.
* `org.infinispan.test.server.extension.libs` locates artifact defined by G:A:V, you can pass a list of libraries (comma separeted) to be copied to the server. Only needed for container mode.
* `org.infinispan.test.server.jdbc.databases` database name to be used during persistence tests.
* `org.infinispan.test.server.jdbc.database.url` JDBC URL. If it's a external database
* `org.infinispan.test.server.jdbc.database.username` database username. If it's a external database
* `org.infinispan.test.server.jdbc.database.password` database password. If it's a external database
* `org.infinispan.test.server.jdbc.database.driverClass` database jdbc driver name. If it's a external database
* `org.infinispan.test.server.jdbc.image.tag` Docker image version to be used during persistence tests.


## JMX

Servers started by the testsuite drivers will have JMX enabled and tests can obtain MBeans by going through the driver API.

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
