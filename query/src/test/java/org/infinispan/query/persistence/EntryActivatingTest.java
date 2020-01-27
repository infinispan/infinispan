package org.infinispan.query.persistence;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.CacheContainer;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.indexedembedded.City;
import org.infinispan.query.indexedembedded.Country;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.persistence.EntryActivatingTest")
public class EntryActivatingTest extends AbstractInfinispanTest {

   Cache<String, Country> cache;
   AdvancedLoadWriteStore store;
   CacheContainer cm;
   SearchManager search;

   @BeforeClass
   public void setUp() {
      recreateCacheManager();
   }

   @AfterClass
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testPersistence() throws PersistenceException {
      verifyFullTextHasMatches(0);

      Country italy = new Country();
      italy.countryName = "Italy";
      City rome = new City();
      rome.name = "Rome";
      italy.cities.add(rome);

      cache.put("IT", italy);
      assert ! store.contains("IT");

      verifyFullTextHasMatches(1);

      cache.evict("IT");
      assert store.contains("IT");

      InternalCacheEntry internalCacheEntry = cache.getAdvancedCache().getDataContainer().get("IT");
      assert internalCacheEntry==null;

      verifyFullTextHasMatches(1);

      Country country = cache.get("IT");
      assert country != null;
      assert "Italy".equals(country.countryName);

      verifyFullTextHasMatches(1);

      cache.stop();
      assert search.unwrap(SearchIntegrator.class).isStopped();
      TestingUtil.killCacheManagers(cm);

      // Now let's check the entry is not re-indexed during data preloading:
      recreateCacheManager();

      // People should generally use a persistent index; we use RAMDirectory for
      // test cleanup, so for our configuration it needs now to contain zero
      // matches: on filesystem it would be exactly one as expected (two when ISPN-1179 was open)
      verifyFullTextHasMatches(0);
   }

   private void recreateCacheManager() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .preload(true)
            .purgeOnStartup(true)
         .indexing()
            .enable()
            .addIndexedEntity(Country.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT")
         ;
      cm = TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
      cache = cm.getCache();
      store = TestingUtil.getFirstLoader(cache);
      search = Search.getSearchManager(cache);
   }

   private void verifyFullTextHasMatches(int i) {
      String query = String.format("FROM %s WHERE countryName:'Italy'", Country.class.getName());
      List<Object> list = search.getQuery(query, IndexedQueryMode.FETCH).list();
      assertEquals(i, list.size());
   }

}
