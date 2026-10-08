package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.sampledomain.bank.Transaction;
import org.infinispan.protostream.sampledomain.bank.User;
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
            .addIndexedEntity(User.ENTITY_NAME)
            .addIndexedEntity(Transaction.ENTITY_NAME)
            .addIndexedEntity("sample_domain.AnalyzerTestEntity")
            .addIndexedEntity("sample_domain.FlightRoute");
      return cfgBuilder;
   }

   @Override
   protected int getNodesCount() {
      return 3;
   }
}
