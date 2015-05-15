package org.infinispan.client.hotrod.impl.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class ReplFailOverRemoteIteratorTest extends BaseIterationFailOverTest {
   @Override
   public ConfigurationBuilder getCacheConfiguration() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }
}
