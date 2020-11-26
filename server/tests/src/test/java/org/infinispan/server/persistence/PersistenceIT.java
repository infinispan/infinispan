package org.infinispan.server.persistence;

import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.DatabaseServerListener;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 10.0
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({
      PooledConnectionOperations.class,
      ManagedConnectionOperations.class,
      JdbcStringBasedCacheStorePassivation.class,
      AsyncJdbcStringBasedCacheStore.class
})
public class PersistenceIT {

   public static final DatabaseServerListener DATABASE = new DatabaseServerListener("h2", "mariadb", "postgres");

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config(System.getProperty(PersistenceIT.class.getName(), "configuration/PersistenceTest.xml"))
                                    .numServers(1)
                                    .runMode(ServerRunMode.CONTAINER)
                                    .mavenArtifacts("com.h2database:h2:1.4.199",
                                                    "org.mariadb.jdbc:mariadb-java-client:2.7.1",
                                                    "org.postgresql:postgresql:jar:42.2.8")
                                    .addListener(DATABASE)
                                    .build();
}
