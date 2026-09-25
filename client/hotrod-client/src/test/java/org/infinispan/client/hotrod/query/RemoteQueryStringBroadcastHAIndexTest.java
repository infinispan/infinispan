package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.sampledomain.bank.Account;
import org.infinispan.protostream.sampledomain.bank.Transaction;
import org.infinispan.protostream.sampledomain.bank.User;
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
      cfgBuilder.indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(User.ENTITY_NAME)
            .addIndexedEntity(Account.ENTITY_NAME)
            .addIndexedEntity(Transaction.ENTITY_NAME)
            .addIndexedEntity("sample_domain.AnalyzerTestEntity")
            .addIndexedEntity("sample_domain.FlightRoute");
      return cfgBuilder;
   }
}
