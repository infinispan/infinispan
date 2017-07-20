package org.infinispan.xsite;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.PessReplCacheOperationsTest")
public class PessReplCacheOperationsTest extends BaseCacheOperationsTest {

   protected ConfigurationBuilder getNycActiveConfig() {
      return getPessimisticReplCache();
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getPessimisticReplCache();
   }

   private ConfigurationBuilder getPessimisticReplCache() {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      dcc.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return dcc;
   }
}
