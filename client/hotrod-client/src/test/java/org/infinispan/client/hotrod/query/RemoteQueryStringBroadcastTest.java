package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests for local indexes in the Hot Rod client.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryStringBroadcastTest")
public class RemoteQueryStringBroadcastTest extends RemoteQueryStringTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      cfgBuilder.indexing().enable().addProperty("default.directory_provider", "local-heap");
      return cfgBuilder;
   }

   @Override
   protected int getNodesCount() {
      return 3;
   }

}
