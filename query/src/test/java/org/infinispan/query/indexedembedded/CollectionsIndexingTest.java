package org.infinispan.query.indexedembedded;

import static org.infinispan.query.dsl.IndexedQueryMode.FETCH;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.indexedembedded.CollectionsIndexingTest")
public class CollectionsIndexingTest extends SingleCacheManagerTest {

   private SearchManager searchManager;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .enable()
            .addIndexedEntity(Country.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   @BeforeClass
   public void prepareSearchManager() {
      searchManager = Search.getSearchManager(cache);
   }

   @AfterMethod
   public void cleanupData() {
      cache.clear();
   }

   @Test
   public void searchOnEmptyIndex() {
      List<?> list = getCountryQuery().list();
      assertEquals(0, list.size());
   }

   private CacheQuery<?> getCountryQuery() {
      String q = String.format("FROM %s where countryName:'Italy'", Country.class.getName());
      return searchManager.getQuery(q, FETCH);
   }

   private CacheQuery<?> getMatchAllQuery() {
      String q = String.format("FROM %s", Country.class.getName());
      return searchManager.getQuery(q, FETCH);
   }

   @Test
   public void searchOnAllTypes() {
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<?> list = getCountryQuery().list();
      assertEquals(1, list.size());
      list = getCountryQuery().list();
      assertEquals(1, list.size());
      list = getMatchAllQuery().list();
      assertEquals(1, list.size());
   }

   @Test
   public void searchOnSimpleField() throws Exception {
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<?> list = getCountryQuery().list();
      assertEquals(1, list.size());
   }

   @Test
   public void searchOnEmbeddedField() {
      Country uk = new Country();
      City london = new City();
      london.name = "London";
      City newcastle = new City();
      newcastle.name = "Newcastle";
      uk.countryName = "United Kingdom";
      uk.cities.add(newcastle);
      uk.cities.add(london);

      //verify behaviour on multiple insertions as well:
      cache.put("UK", uk);
      cache.put("UK", uk);
      cache.put("UK", uk);
      String q = String.format("FROM %s c where c.cities.name:'Newcastle'", Country.class.getName());
      List<?> list = searchManager.getQuery(q, FETCH).list();
      assertEquals(1, list.size());
      assertSame(uk, list.get(0));
   }
}
