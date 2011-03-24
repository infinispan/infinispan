package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.tx.dld.DldEagerLockingDistributedTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronization.DldEagerLockingDistributedWithSyncTest")
public class DldEagerLockingDistributedWithSyncTest extends DldEagerLockingDistributedTest {
   @Override
   protected Configuration createConfiguration() {
      Configuration configuration = super.createConfiguration();
      configuration.configureTransaction().useSynchronization(true);
      return configuration;
   }
}
