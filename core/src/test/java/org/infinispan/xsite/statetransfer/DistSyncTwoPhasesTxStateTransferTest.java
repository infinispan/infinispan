package org.infinispan.xsite.statetransfer;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests the cross-site replication with concurrent operations checking for consistency using a distributed synchronous
 * cache with two-phases backup.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.DistSyncTwoPhasesTxStateTransferTest")
public class DistSyncTwoPhasesTxStateTransferTest extends BaseStateTransferTest {

   public DistSyncTwoPhasesTxStateTransferTest() {
      super();
      use2Pc = true;
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
