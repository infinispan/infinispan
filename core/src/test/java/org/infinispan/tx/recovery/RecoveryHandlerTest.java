package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
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
      final Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      config.fluent().transaction().recovery().useSynchronization(false);
      createCluster(config, 2);
      waitForClusterToForm();
   }

   public void testRecoveryHandler() throws Exception {
      final XAResource xaResource = cache(0).getAdvancedCache().getXAResource();
      final Xid[] recover = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
      assert recover != null && recover.length == 0;
   }
}
