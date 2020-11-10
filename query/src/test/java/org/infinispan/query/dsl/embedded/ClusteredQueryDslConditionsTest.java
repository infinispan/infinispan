package org.infinispan.query.dsl.embedded;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingConfigurationBuilder;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.testng.annotations.Test;

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
      createClusteredCaches(2, DslSCI.INSTANCE, defaultConfiguration);

      ConfigurationBuilder cfg = initialCacheConfiguration();
      IndexingConfigurationBuilder indexingConfigurationBuilder = cfg.clustering()
            .stateTransfer().fetchInMemoryState(true)
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(getModelFactory().getUserImplClass())
            .addIndexedEntity(getModelFactory().getAccountImplClass())
            .addIndexedEntity(getModelFactory().getTransactionImplClass());

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

   private void checkIndexPresence(Cache<?, ?> cache) {
      SearchMapping searchMapping = TestQueryHelperFactory.extractSearchMapping(cache);

      verifyClassIsIndexed(searchMapping, getModelFactory().getUserImplClass());
      verifyClassIsIndexed(searchMapping, getModelFactory().getAccountImplClass());
      verifyClassIsIndexed(searchMapping, getModelFactory().getTransactionImplClass());
      verifyClassIsNotIndexed(searchMapping, getModelFactory().getAddressImplClass());
   }

   private void verifyClassIsNotIndexed(SearchMapping searchMapping, Class<?> type) {
      assertThat(searchMapping.allIndexedTypes()).doesNotContainValue(type);
   }

   private void verifyClassIsIndexed(SearchMapping searchMapping, Class<?> type) {
      assertThat(searchMapping.allIndexedTypes()).containsValue(type);
   }
}
