package org.infinispan.xsite.statetransfer;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests the cross-site replication with concurrent operations checking for consistency using a distributed synchronous
 * transaction cache with one-phase backup.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.DistSyncOnePhaseTxStateTransferTest")
public class DistSyncOnePhaseTxStateTransferTest extends BaseStateTransferTest {

   public DistSyncOnePhaseTxStateTransferTest() {
      super();
      use2Pc = false;
      implicitBackupCache = true;
      transactional = true;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(cacheMode, transactional);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(cacheMode, transactional);
   }
}
