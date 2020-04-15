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
      cfgBuilder.indexing().enable()
                .addIndexedEntity("sample_bank_account.User")
                .addIndexedEntity("sample_bank_account.Account")
                .addIndexedEntity("sample_bank_account.Transaction")
                .addIndexedEntity("sample_bank_account.AnalyzerTestEntity")
                .addProperty("directory.type", "local-heap");
      return cfgBuilder;
   }
}
