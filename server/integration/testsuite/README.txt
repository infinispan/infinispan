Running the testsuite
---------------------

  mvn clean verify (runs all tests)

Running a subset of the testsuite
---------------------------------
Subsets of the testsuite are specified by profiles that switch off running of certain test classes or the whole failsafe plugin executions.
Currently these subsets are predefined:

  -P suite.client                    (Client tests, all local/dist/repl cachemode)
  -P suite.rolling.upgrades          (Rolling upgrades specific tests, mandatory specification of:
                                      -Dzip.dist.old=path/to/old_distro.zip

                                      NOTE: there are 2 properties defined with default values:
                                      1) -Dold.server.schema.version=6.1 -- used for failsafe report suffix
                                      2) -Dnew.server.schema.version=7.0 -- used to decide which snippet to use during
                                          transformation of a configuration for new servers in the clustered scenario
                                      Configuration snippets for new cluster are prepared for versions: 6.0, 6.1, 7.0)

                                      Testing backwards compatibility: use "old" -Dhotrod.protocol.version to set
                                      HR protocol version for communication with "new" servers.

                                      Settings are also applicable/madatory for other Rolling Upgrades profiles bellow.

  -P suite.rolling.upgrades.dist     (Distribution case of Rolling upgrades)

  -P suite.rolling.upgrades.jbossas       (This profile needs to be used when running Rolling upgrades with
                                           old Infinispan server based on JBoss AS)
  -P suite.rolling.upgrades.dist.jbossas  (Distribution case of Rolling upgrades with
                                           old Infinispan server based on JBoss AS)
  -P smoke                             (Smoke tests. A small subset of server mode tests)

Running with specific server zip distribution
---------------------------------------------

If you specify -Dzip.dist=path/to/distro.zip the test server directories target/server/node* will be based on the contents of this zip file.


Running specific test 
---------------------
When running only a specific test, it's important to realize that by default, there are multiple executions of the maven-failsafe-plugin defined
and therefore the test might be executed multiple times even if it's specified via -Dit.test= option. e.g.

  mvn clean verify -Dit.test=org/infinispan/server/test/client/hotrod/HotRodRemoteCacheIT#testPut
  will run the testPut method three times, each time in different clustering mode (local/dist/repl)

so besides the -Dit.test= directive it's useful to specify also the most specific suite for the given test:

  mvn clean verify -P suite.client.local -Dit.test=org/infinispan/server/test/client/hotrod/HotRodRemoteCacheIT#testPut
  will run the testPut method only once for the local cache mode.

Also note that integration test classes need to be named ending "IT", not "TEST", as required by the maven-failsafe-plugin.


Running tests in OSGi
---------------------------------
This is controlled by following profile

  -P client.hotrod.osgi    These tests manage an Infinispan server through the maven-antrun-plugin.

  Tests for OSGi run by default in Karaf 2.3.3. A different version of Karaf may be specified via the command line:
  -Dversion.karaf=<version>

Running client tests with TCP stack (UDP by default)
----------------------------------------------------
Controlled by property infinispan.test.jgroups.protocol:
  mvn clean verify -Psuite.client -Dinfinispan.test.jgroups.protocol=tcp

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

1. -Dtrace=org.infinispan.category1,org.jgroups.category2

What it does:
    <console-handler name="CONSOLE">
        <level name="INFO"/>
        <formatter>
            <named-formatter name="COLOR-PATTERN"/>
        </formatter>
    </console-handler>

    <file-handler name="FILE">
        <level name="TRACE""/>
        <formatter>
            <named-formatter name="PATTERN"/>
        </formatter>
        <file relative-to="jboss.server.log.dir" path="server.log"/>
        <append value="true"/>
    </file-handler>
    <logger category="org.infinispan.category1">
        <level name="TRACE"/>
    </logger>
    <logger category="org.jgroups.category2">
        <level name="TRACE"/>
    </logger>
INFO: We do not set TRACE level for console logger here because multiple servers are running and it slows down test execution

RocksDB specifics
-----------------
properties:

  rocksdb.compression - sets compression type, allowed values: SNAPPY, NONE, ZLIB, BZ2LIB, LZ4, LZ4HC

