package org.infinispan.query.persistence;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.CacheContainer;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.indexedembedded.City;
import org.infinispan.query.indexedembedded.Country;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.search.mapper.mapping.SearchMapping;
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
   WaitNonBlockingStore store;
   CacheContainer cm;
   QueryFactory queryFactory;
   SearchMapping searchMapping;

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
      assert !store.contains("IT");

      verifyFullTextHasMatches(1);

      cache.evict("IT");
      assert store.contains("IT");

      InternalCacheEntry internalCacheEntry = cache.getAdvancedCache().getDataContainer().get("IT");
      assert internalCacheEntry == null;

      verifyFullTextHasMatches(1);

      Country country = cache.get("IT");
      assert country != null;
      assert "Italy".equals(country.countryName);

      verifyFullTextHasMatches(1);

      cache.stop();
      assert searchMapping.isClose();
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
         .indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Country.class)
         ;
      cm = TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
      cache = cm.getCache();
      store = TestingUtil.getFirstStore(cache);
      searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);
   }

   private void verifyFullTextHasMatches(int i) {
      String query = String.format("FROM %s WHERE countryName:'Italy'", Country.class.getName());
      List<Object> list = cache.query(query).list();
      assertEquals(i, list.size());
   }

}
