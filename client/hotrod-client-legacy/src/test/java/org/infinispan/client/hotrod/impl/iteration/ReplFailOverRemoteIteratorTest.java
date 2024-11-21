package org.infinispan.client.hotrod.impl.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.ReplFailOverRemoteIteratorTest")
public class ReplFailOverRemoteIteratorTest extends BaseIterationFailOverTest {
   @Override
   public ConfigurationBuilder getCacheConfiguration() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }
}
