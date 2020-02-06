package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests for query Broadcasting when using DIST caches with Index.ALL
 *
 * @since 10.1
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryStringBroadcastHAIndexTest")
public class RemoteQueryStringBroadcastHAIndexTest extends RemoteQueryStringBroadcastTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      cfgBuilder.indexing().enable().addProperty("default.directory_provider", "local-heap");
      return cfgBuilder;
   }

}
