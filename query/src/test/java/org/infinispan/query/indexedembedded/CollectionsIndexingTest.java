package org.infinispan.query.indexedembedded;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

import java.util.List;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
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
   public void searchOnEmptyIndex() throws Exception {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");
      Query query = queryParser.parse("Italy");
      List<?> list = searchManager.getQuery(query, Country.class, City.class).list();
      assertEquals(0, list.size());
   }

   @Test
   public void searchOnAllTypes() throws Exception {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");
      Query query = queryParser.parse("Italy");
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<?> list = searchManager.getQuery(query, Country.class, City.class).list();
      assertEquals(1, list.size());
      list = searchManager.getQuery(query).list();
      assertEquals(1, list.size());
      list = searchManager.getQuery(new MatchAllDocsQuery()).list();
      assertEquals(1, list.size());
   }

   @Test
   public void searchOnSimpleField() throws Exception {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");
      Query query = queryParser.parse("Italy");
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<?> list = searchManager.getQuery(query, Country.class, City.class).list();
      assertEquals(1, list.size());
   }

   @Test
   public void searchOnEmbeddedField() throws Exception {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("cities.name");
      Query query = queryParser.parse("Newcastle");

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
      List<?> list = searchManager.getQuery(query, Country.class, City.class).list();
      assertEquals(1, list.size());
      assertSame(uk, list.get(0));
   }
}
