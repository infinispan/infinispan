package org.infinispan.tx.recovery;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.recovery.RecoveryHandlerTest")
public class RecoveryHandlerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      config.transaction().useSynchronization(false).recovery().enable();
      createCluster(config, 2);
      waitForClusterToForm();
   }

   public void testRecoveryHandler() throws Exception {
      final XAResource xaResource = cache(0).getAdvancedCache().getXAResource();
      final Xid[] recover = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
      assert recover != null && recover.length == 0;
   }
}
