package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

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
      cfgBuilder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("sample_bank_account.User")
            .addIndexedEntity("sample_bank_account.Transaction")
            .addIndexedEntity("sample_bank_account.AnalyzerTestEntity");
      return cfgBuilder;
   }

   @Override
   protected int getNodesCount() {
      return 3;
   }
}
