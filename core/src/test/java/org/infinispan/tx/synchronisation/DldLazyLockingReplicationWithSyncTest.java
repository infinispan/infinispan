package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.tx.dld.DldLazyLockingReplicationTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronization.DldLazyLockingReplicationWithSyncTest")
public class DldLazyLockingReplicationWithSyncTest extends DldLazyLockingReplicationTest {

   @Override
   protected Configuration createConfiguration() {
      Configuration configuration = super.createConfiguration();
      configuration.configureTransaction().useSynchronization(true);
      return configuration;
   }
}
