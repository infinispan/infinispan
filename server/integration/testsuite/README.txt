Running the testsuite
---------------------

  mvn clean verify (runs all tests)

Running a subset of the testsuite
---------------------------------
Subsets of the testsuite are specified by profiles that switch off running of certain test classes or the whole surefire plugin executions.
Currently these subsets are predefined:

  -P suite.client                    (Client tests, all local/dist/repl cachemode)
  -P suite.client.{local|dist|repl}  (Client tests, only {local|dist|repl} cachemode)
  -P suite.examples                  (Example config tests)
  -P suite.leveldb-client            (LevelDB cache store tests - the whole suite.client with leveldb configs)
  -P suite.cachestore                (Tests that use different cachestore configurations, remote, leveldb apod)
  -P suite.rolling.upgrades          (Rolling upgrades specific tests, mandatory specification of: -Dzip.dist.old=path/to/old_distro.zip)

  -P suite.others                    (Tests that do not belong to any of the suites above. Useful when running a single test that's outside
                                      of any pre-defined group)

Runnins with specific server zip distribution
---------------------------------------------

If you specify -Dzip.dist=path/to/distro.zip the test server directories target/server/node* will be based on the contents of this zip file.


Running specific test 
---------------------
When running only a specific test, it's important to realize that by default, there are multiple executions of the maven-surefire-plugin defined 
and therefore the test might be executed multiple times even if it's specified via -Dtest= option. e.g.

  mvn clean verify -Dtest=org/infinispan/server/test/client/hotrod/HotRodRemoteCacheTest#testPut
  will run the testPut method three times, each time in different clustering mode (local/dist/repl)

so besides the -Dtest= directive it's useful to specify also the most specific suite for the given test:

  mvn clean verify -P suite.client.local -Dtest=org/infinispan/server/test/client/hotrod/HotRodRemoteCacheTest#testPut
  will run the testPut method only once for the local cache mode.

Running tests for specific client 
---------------------------------
This is controlled by following profiles

  -P client.rest      (REST client)
  -P client.memcached (Memcached client)
  -P client.hotrod    (Hot Rod client)

Running client tests with TCP stack (UDP by default)
----------------------------------------------------
Controlled by property default.transport.stack:
  mvn clean verify -Psuite.client -Ddefault.transport.stack=tcp

Client side logging
-------------------

The testsuite uses Log4j for logging and the logging config is in src/test/resources/log4j.xml
The file output goes by default to file infinispan-server.log 

Server side logging
-------------------

The server logs will be stored in the standard location of the test distributions:

  target/server/node1/standalone/log/server.log
  target/server/node2/standalone/log/server.log
  target/server/node3/standalone/log/server.log

Test suite allows generic logging level change via command line parameters.
Available parameters:

1. -Dlog.level.infinispan=[loggingLevel]
2. -Dlog.level.jgroups=[loggingLevel]
3. -Dlog.level.console=[loggingLevel]

What it does:
1 - sets subsystem/logger[@category='org.infinispan']/level[@name] to [loggingLevel], INFO by default
2 - sets subsystem/logger[@category='org.jgroups']/level[@name] to [loggingLevel], INFO by default
3 - sets subsystem/console-handler[@name = 'CONSOLE']/level[@name] to [loggingLevel], INFO by default

When 1) or 2) is set (only one of them), it also changes subsystem/periodic-rotating-file-handler[@name = 'FILE']/level[@name]
to [loggingLevel], INFO by default. When both 1) and 2) is provided, subsystem/periodic-rotating-file-handler[@name = 'FILE']/level[@name] is set
to TRACE.

LevelDB specifics
-----------------
properties:

  leveldb.compression - sets compression type, allowed values: SNAPPY, NONE
  leveldb.impl        - sets implementation type, allowed values: AUTO, JAVA, JNI
  leveldb.patch       - used with -Dzip.dist. Patches the zip distribution with dependencies of leveldb cache store taken from upstream build.

Runnig test in JDK 6
--------------------

When want to run testsuite on JDK 6, you have to set following profile

  -P testsuite-jdk6

This profile assumes that environment JAVA_HOME_16 is set properly.
  
