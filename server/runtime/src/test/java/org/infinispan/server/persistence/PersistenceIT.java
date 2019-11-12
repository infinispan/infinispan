package org.infinispan.server.persistence;

import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
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

   @ClassRule
   public static InfinispanServerRule SERVERS = new InfinispanServerRule(
         new InfinispanServerRuleConfigurationBuilder("configuration/ClusteredServerTest.xml")
            .numServers(1)
            .serverRunMode(ServerRunMode.CONTAINER)
            .mavenArtifacts(new String[] {"com.h2database:h2:1.4.199", "mysql:mysql-connector-java:8.0.17", "org.postgresql:postgresql:jar:42.2.8"})
   );
}
