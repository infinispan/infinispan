package org.infinispan.server.rollingupgrade;

import org.infinispan.server.persistence.AsyncJdbcStringBasedCacheStore;
import org.infinispan.server.persistence.BaseJdbcStringBasedCacheStoreIT;
import org.infinispan.server.persistence.BasePooledConnectionOperations;
import org.infinispan.server.persistence.ManagedConnectionOperations;
import org.infinispan.server.persistence.PersistenceIT;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.infinispan.server.test.junit5.RollingUpgradeHandlerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite(failIfNoTests = false)
@SelectClasses({
      BasePooledConnectionOperations.class,
      ManagedConnectionOperations.class,
      BaseJdbcStringBasedCacheStoreIT.class,
      AsyncJdbcStringBasedCacheStore.class
})
public class PersistenceRollingUpgradeIT extends InfinispanSuite {

   @RegisterExtension
   public static RollingUpgradeHandlerExtension SERVERS =
         RollingUpgradeHandlerExtension.from(PersistenceRollingUpgradeIT.class, PersistenceIT.EXTENSION_BUILDER, "15.2.0.Final", "15.2.1.Final");
}
