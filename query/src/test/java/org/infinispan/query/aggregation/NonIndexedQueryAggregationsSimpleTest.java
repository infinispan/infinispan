package org.infinispan.query.aggregation;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.IndexedPlayer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.aggregation.NonIndexedQueryAggregationsSimpleTest")
public class NonIndexedQueryAggregationsSimpleTest extends QueryAggregationsSimpleTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createCacheManager(IndexedPlayer.PlayerSchema.INSTANCE, new ConfigurationBuilder());
   }
}
