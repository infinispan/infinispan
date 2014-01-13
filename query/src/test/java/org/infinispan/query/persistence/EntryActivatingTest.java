package org.infinispan.query.persistence;

import java.util.List;

import junit.framework.Assert;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.indexedembedded.City;
import org.infinispan.query.indexedembedded.Country;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.persistence.EntryActivatingTest")
public class EntryActivatingTest extends AbstractInfinispanTest {

   Cache<String, Country> cache;
   AdvancedLoadWriteStore store;
   CacheContainer cm;
   SearchManager search;
   QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");

   @BeforeTest
   public void setUp() {
      recreateCacheManager();
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testPersistence() throws PersistenceException, ParseException {
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
      assert ((SearchFactoryIntegrator)search.getSearchFactory()).isStopped();
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
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT")
         ;
      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      store = (AdvancedLoadWriteStore) TestingUtil.getFirstLoader(cache);
      search = Search.getSearchManager(cache);
   }

   private void verifyFullTextHasMatches(int i) throws ParseException {
      Query query = queryParser.parse("Italy");
      List<Object> list = search.getQuery(query, Country.class, City.class).list();
      Assert.assertEquals( i , list.size() );
   }

}
