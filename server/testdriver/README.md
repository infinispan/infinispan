git# Infinispan Server Test Driver

The Infinispan Server test driver is built around the following tools:

* JUnit 4.x
* Testcontainers 1.12.x

The general aim of the driver is to reduce server churn by reusing servers between tests that can share the same
configuration. Tests should therefore be grouped under suites which take care of configuring, starting and stopping the
servers.

## Naming and layout conventions

* Suites and individual tests that cannot be run as part of a suite must have names ending with `Test` or `IT`.
* Tests which are part of suites **must not** have names ending with `Test` or `IT`. They can still be executed
  individually either from the IDE or from the command-line using Surefire's `-Dtest=TestName` or `-Dit.test=ITName`.
* Each suite together with all of its tests must be placed in a distinct directory.
* Configuration files are modularized using XInclude so that sections can be reused across multiple configurations
  without using XSLT trickery.

## Server run modes (aka Drivers)

The test driver can run Infinispan Servers in different modes:

* **EMBEDDED**: all servers are run in the same JVM as the test
* **CONTAINER**: the servers are executed inside a container
* **FORKED**: the servers are executed in a different JVM process

The **CONTAINER** driver can either use a user-supplied image, a published one or a local server installation.

These are all handled by a specific driver which takes care of:

* setting up the server layout
* creating a CA and server and client certificates and assembling them into keystores/truststores
* creating user / role property files for authentication tests

The driver used by tests can be chosen by setting the system property `org.infinispan.test.server.driver` to one of the
above names.
By default, the **EMBEDDED** driver will be used. When running the testsuite through Maven, you can force the use of the
**CONTAINER** driver by using the `container` profile:

```
mvn -Pcontainer verify
```

## JUnit 4.x Rules

The testsuite comes with a number of JUnit 4.x rules which aid in the setup of server clusters.

### InfinispanServerRule

This `@ClassRule` must be used in both suites and individual tests. It specifies the configuration used by the servers
and the number of nodes.
It creates a server directory layout for each node under `/tmp/infinispanTempFiles/fully.qualified.test.ClassName`. It
then starts all nodes and waits for the cluster to form. After all test methods / classes in the suite have been
executed, it stops all the servers.
When used in suites, it needs to be initialized in the suite class:

```java

@RunWith(Suite.class)
@Suite.SuiteClasses({
    A.class,
    B.class
})
public class LotsOfTests {

    @ClassRule
    public static final InfinispanServerRule SERVERS = InfinispanServerRuleBuilder.config("config.xml").numServers(2).build();
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

## Elasticity / Resilience tests

You can pause/resume/start/stop nodes during the lifecycle of a test. This is only possible when running with the
CONTAINER driver. From a test, invoke:

```java
SERVERS.getServerDriver().

pause(0); // Sends a SIGSTOP to the specified server node
// ...
SERVERS.

getServerDriver().

resume(0); // Sends a SIGCONT to the specified server node
```

to pause a node and subsequently resume it.

## Testsuite Properties

The following is a list of properties which affect the build:

* `infinispan.cluster.stack` the name of the JGroups stack. Defaults to `test-tcp`.
* `org.infinispan.test.server.container.baseImageName` the base image to use for the server.
   Defaults to `eclipse-temurin:25-ubi10-minimal`.
* `org.infinispan.test.server.container.usePrebuiltServer` whether to use a prebuilt server from the supplied image above.
* `org.infinispan.test.server.container.preserveImage` whether to preserve the created image after the test has run.
* `org.infinispan.test.server.container.timeoutSeconds` the amount of time in seconds to wait for a server start/stop
  operation when using the `CONTAINER` driver.
* `org.infinispan.test.server.embedded.timeoutSeconds` the amount of time in seconds to wait for a server start/stop
  operation when using the `EMBEDDED` driver.
* `org.infinispan.test.server.driver`  the driver to use, `EMBEDDED`, `CONTAINER` or `FORKED`. Defaults to the
  `EMBEDDED` driver.
* `org.infinispan.test.server.extension.libs` locates artifact defined by G:A:V, you can pass a list of libraries (comma
  separeted) to be copied to the server. Only needed for container mode.
* `org.infinispan.test.server.database.types` comma-separated list of database types to be used during persistence
  tests.
* `org.infinispan.test.server.database.properties.path` a path to a directory containing property files with connection
  details for each database to be tested.
* `org.infinispan.server.test.database.<type>.username` username to use for a specific database type
* `org.infinispan.server.test.database.<type>.password` password to use for a specific database type
* `org.infinispan.server.test.database.<type>.address` address to use for a specific database type
* `org.infinispan.server.test.database.<type>.port` port to use for a specific database type
* `org.infinispan.test.server.container.timeoutSeconds` the amount of time in seconds to wait for a server start/stop
  operation when using the `FORKED` driver
* `org.infinispan.test.server.home` Specifies a comma-separated list to server home path. Only for `FORKED` driver
* `org.infinispan.test.server.http.timeout` Specifies a timeout in seconds for HTTP connections (defaults to 10)

## JMX

Servers started by the testsuite drivers will have JMX enabled and tests can obtain MBeans by going through the driver
API.