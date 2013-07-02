package org.infinispan.xsite.backupfailure.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.backupfailure.tx.ReplLocalClusterFailureTest")
public class ReplLocalClusterFailureTest extends BaseLocalClusterTxFailureTest {

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
   }
}
