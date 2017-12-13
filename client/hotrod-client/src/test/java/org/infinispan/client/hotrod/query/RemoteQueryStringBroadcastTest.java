package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.testng.annotations.Test;

/**
 * Tests for {@link org.infinispan.query.dsl.IndexedQueryMode} from the Hot Rod client.
 * Each node has a local index, and the client creates a query with BROADCAST to return
 * correct results.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryStringBroadcastTest")
public class RemoteQueryStringBroadcastTest extends RemoteQueryStringTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      cfgBuilder.indexing().index(Index.PRIMARY_OWNER).addProperty("default.directory_provider", "local-heap");
      return cfgBuilder;
   }

   @Override
   protected int getNodesCount() {
      return 3;
   }

   @Override
   protected Query createQueryFromString(String q) {
      return getQueryFactory().create(q, IndexedQueryMode.BROADCAST);
   }
}
