package org.infinispan.server.persistence;

import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.core.ServerRunMode;
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
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
                                    .numServers(1)
                                    .runMode(ServerRunMode.CONTAINER)
                                    .mavenArtifacts("com.h2database:h2:1.4.199",
                                                    "mysql:mysql-connector-java:8.0.17",
                                                    "org.postgresql:postgresql:jar:42.2.8")
                                    .build();
}
