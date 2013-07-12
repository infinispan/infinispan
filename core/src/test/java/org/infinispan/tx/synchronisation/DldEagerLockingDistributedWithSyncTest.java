package org.infinispan.tx.synchronisation;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.tx.dld.DldPessimisticLockingDistributedTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronisation.DldEagerLockingDistributedWithSyncTest")
public class DldEagerLockingDistributedWithSyncTest extends DldPessimisticLockingDistributedTest {
   @Override
   protected ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder configuration = super.createConfiguration();
      configuration.transaction().useSynchronization(true);
      return configuration;
   }
}
