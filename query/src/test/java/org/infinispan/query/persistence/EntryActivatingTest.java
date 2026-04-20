package org.infinispan.query.persistence;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.query.indexedembedded.City;
import org.infinispan.query.indexedembedded.Country;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.persistence.EntryActivatingTest")
public class EntryActivatingTest extends SingleCacheManagerTest {

   WaitNonBlockingStore store;
   SearchMapping searchMapping;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
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
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      store = TestingUtil.getFirstStore(cache);
      searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);
   }

   public void testPersistence() throws Exception {
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

      InternalCacheEntry internalCacheEntry = cache.getAdvancedCache().getDataContainer().peek("IT");
      assert internalCacheEntry == null;

      verifyFullTextHasMatches(1);

      Country country = (Country) cache.get("IT");
      assert country != null;
      assert "Italy".equals(country.countryName);

      verifyFullTextHasMatches(1);

      cache.stop();
      assert searchMapping.isClose();
      teardown();

      // Now let's check the entry is not re-indexed during data preloading:
      setup();

      // People should generally use a persistent index; we use RAMDirectory for
      // test cleanup, so for our configuration it needs now to contain zero
      // matches: on filesystem it would be exactly one as expected (two when ISPN-1179 was open)
      verifyFullTextHasMatches(0);
   }

   private void verifyFullTextHasMatches(int i) {
      String query = String.format("FROM %s WHERE countryName:'Italy'", Country.class.getName());
      List<Object> list = cache.query(query).list();
      assertEquals(i, list.size());
   }
}
