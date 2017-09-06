package org.infinispan.query.dsl.embedded;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.IndexingConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
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

   protected Map<String, String> getIndexConfig() {
      Map<String, String> configs = new HashMap<>();
      configs.put("default.indexmanager", InfinispanIndexManager.class.getName());
      configs.put("lucene_version", "LUCENE_CURRENT");
      return configs;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      defaultConfiguration.clustering()
            .stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(2, defaultConfiguration);

      ConfigurationBuilder cfg = initialCacheConfiguration();
      IndexingConfigurationBuilder indexingConfigurationBuilder = cfg.clustering()
            .stateTransfer().fetchInMemoryState(true)
            .indexing()
            .index(Index.PRIMARY_OWNER)
            .addIndexedEntity(getModelFactory().getUserImplClass())
            .addIndexedEntity(getModelFactory().getAccountImplClass())
            .addIndexedEntity(getModelFactory().getTransactionImplClass());

      getIndexConfig().forEach(indexingConfigurationBuilder::addProperty);

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
      SearchIntegrator searchIntegrator = Search.getSearchManager(cache).unwrap(SearchIntegrator.class);

      verifyClassIsIndexed(searchIntegrator, getModelFactory().getUserImplClass());
      verifyClassIsIndexed(searchIntegrator, getModelFactory().getAccountImplClass());
      verifyClassIsIndexed(searchIntegrator, getModelFactory().getTransactionImplClass());
      verifyClassIsNotIndexed(searchIntegrator, getModelFactory().getAddressImplClass());
   }

   private void verifyClassIsNotIndexed(SearchIntegrator searchIntegrator, Class<?> type) {
      assertFalse(searchIntegrator.getIndexBindings().containsKey(PojoIndexedTypeIdentifier.convertFromLegacy(type)));
      assertNull(searchIntegrator.getIndexManager(type.getName()));
   }

   private void verifyClassIsIndexed(SearchIntegrator searchIntegrator, Class<?> type) {
      assertTrue(searchIntegrator.getIndexBindings().containsKey(PojoIndexedTypeIdentifier.convertFromLegacy(type)));
      assertNotNull(searchIntegrator.getIndexManager(type.getName()));
   }
}
