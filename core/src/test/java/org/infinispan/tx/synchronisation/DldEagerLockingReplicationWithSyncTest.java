package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.tx.dld.DldEagerLockingReplicationTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronization.DldEagerLockingReplicationWithSyncTest")
public class DldEagerLockingReplicationWithSyncTest extends DldEagerLockingReplicationTest {
   @Override
   protected Configuration getConfiguration() throws Exception {
      Configuration config = super.getConfiguration();
      config.configureTransaction().useSynchronization(true);
      return config;
   }
}
