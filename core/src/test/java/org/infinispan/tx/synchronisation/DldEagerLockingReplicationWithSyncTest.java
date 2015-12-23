package org.infinispan.tx.synchronisation;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.tx.dld.DldPessimisticLockingReplicationTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronisation.DldEagerLockingReplicationWithSyncTest")
public class DldEagerLockingReplicationWithSyncTest extends DldPessimisticLockingReplicationTest {

   @Override
   protected ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder config = super.createConfiguration();
      config.transaction().useSynchronization(true);
      return config;
   }
}
