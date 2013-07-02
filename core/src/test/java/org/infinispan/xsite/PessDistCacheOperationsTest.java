package org.infinispan.xsite;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.PessDistCacheOperationsTest")
public class PessDistCacheOperationsTest extends BaseDistCacheOperationsTest {

   protected ConfigurationBuilder getNycActiveConfig() {
      return getPessimisticDistCache();
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getPessimisticDistCache();
   }

   private ConfigurationBuilder getPessimisticDistCache() {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return dcc;
   }
}
