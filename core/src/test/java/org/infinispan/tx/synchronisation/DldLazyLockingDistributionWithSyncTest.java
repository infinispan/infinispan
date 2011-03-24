package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.tx.dld.DldLazyLockingDistributionTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.synchronization.DldEagerLockingDistWithSyncTest")
public class DldLazyLockingDistributionWithSyncTest extends DldLazyLockingDistributionTest {

   @Override
   protected Configuration updatedConfig() {
      Configuration configuration = super.updatedConfig();
      configuration.configureTransaction().useSynchronization(true);
      return configuration;
   }
}
