package org.infinispan.query.dsl.embedded;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.Search;
import org.testng.annotations.Test;

import static org.junit.Assert.*;

/**
 * Verifies the functionality of Query DSL in clustered environment for ISPN directory provider.
 *
 * @author anistor@redhat.com
 * @author Anna Manukyan
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.ClusteredQueryDslConditionsTest")
public class ClusteredQueryDslConditionsTest extends QueryDslConditionsTest {

   protected static final String TEST_CACHE_NAME = "custom";

   protected Cache<Object, Object> cache1, cache2;

   @Override
   protected Cache<Object, Object> getCacheForWrite() {
      return cache1;
   }

   @Override
   protected Cache<Object, Object> getCacheForQuery() {
      return cache2;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      defaultConfiguration.clustering()
            .stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(2, defaultConfiguration);

      ConfigurationBuilder cfg = initialCacheConfiguration();
      cfg.clustering()
            .stateTransfer().fetchInMemoryState(true)
            .indexing()
            .index(Index.LOCAL)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager");

      manager(0).defineConfiguration(TEST_CACHE_NAME, cfg.build());
      manager(1).defineConfiguration(TEST_CACHE_NAME, cfg.build());
      cache1 = manager(0).getCache(TEST_CACHE_NAME);
      cache2 = manager(1).getCache(TEST_CACHE_NAME);
   }

   protected ConfigurationBuilder initialCacheConfiguration() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }

   @Override
   public void testIndexPresence() {
      checkIndexPresence(cache1);
      checkIndexPresence(cache2);
   }

   private void checkIndexPresence(Cache cache) {
      SearchIntegrator searchFactory = Search.getSearchManager(cache).unwrap(SearchIntegrator.class);

      assertTrue(searchFactory.getIndexedTypes().contains(getModelFactory().getUserImplClass()));
      assertNotNull(searchFactory.getIndexManager(getModelFactory().getUserImplClass().getName()));

      assertTrue(searchFactory.getIndexedTypes().contains(getModelFactory().getAccountImplClass()));
      assertNotNull(searchFactory.getIndexManager(getModelFactory().getAccountImplClass().getName()));

      assertTrue(searchFactory.getIndexedTypes().contains(getModelFactory().getTransactionImplClass()));
      assertNotNull(searchFactory.getIndexManager(getModelFactory().getTransactionImplClass().getName()));

      assertFalse(searchFactory.getIndexedTypes().contains(getModelFactory().getAddressImplClass()));
      assertNull(searchFactory.getIndexManager(getModelFactory().getAddressImplClass().getName()));
   }
}
