package org.infinispan.xsite.backupfailure.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.backupfailure.tx.DistLocalClusterTxBackupFailureTest")
public class DistLocalClusterTxBackupFailureTest extends BaseLocalClusterTxFailureTest {

   public DistLocalClusterTxBackupFailureTest() {
      isLonBackupTransactional = true;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }
}
