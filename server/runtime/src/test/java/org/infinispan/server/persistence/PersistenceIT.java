package org.infinispan.server.persistence;

import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestConfiguration;
import org.infinispan.server.test.ServerRunMode;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 10.0
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({
      PooledConnectionOperations.class
})
public class PersistenceIT {

   protected static final Integer NUM_SERVERS = 1;

   @ClassRule
   public static InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerTestConfiguration("configuration/ClusteredServerTest.xml")
         .numServers(NUM_SERVERS).runMode(ServerRunMode.CONTAINER).mavenArtifacts("com.h2database:h2:1.4.199", "mysql:mysql-connector-java:8.0.17", "org.postgresql:postgresql:jar:42.2.8"));
}
