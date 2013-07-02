package org.infinispan.xsite;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.NonTxReplCacheOperationsTest")
public class NonTxReplCacheOperationsTest extends BaseCacheOperationsTest {

   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }
}
