package org.infinispan.query.indexedembedded;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.indexedembedded.CollectionsIndexingTest")
public class CollectionsIndexingTest extends SingleCacheManagerTest {

   private SearchManager qf;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .index(Index.ALL)
            .addIndexedEntity(Country.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeClass
   public void prepareSearchFactory() throws Exception {
      qf = Search.getSearchManager(cache);
   }

   @AfterMethod
   public void cleanupData() {
      cache.clear();
   }

   @Test
   public void searchOnEmptyIndex() throws ParseException {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");
      Query query = queryParser.parse("Italy");
      List<Object> list = qf.getQuery(query, Country.class, City.class).list();
      assertEquals(0, list.size());
   }

   @Test
   public void searchOnAllTypes() throws ParseException {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");
      Query query = queryParser.parse("Italy");
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<Object> list = qf.getQuery(query, Country.class, City.class).list();
      assertEquals(1, list.size());
      list = qf.getQuery(query).list();
      assertEquals(1, list.size());
      list = qf.getQuery(new MatchAllDocsQuery()).list();
      assertEquals(1, list.size());
   }

   @Test
   public void searchOnSimpleField() throws ParseException {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");
      Query query = queryParser.parse("Italy");
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<Object> list = qf.getQuery(query, Country.class, City.class).list();
      assertEquals(1, list.size());
   }

   @Test
   public void searchOnEmbeddedField() throws ParseException {
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
      List<Object> list = qf.getQuery(query, Country.class, City.class).list();
      assertEquals(1, list.size());
      assertTrue(uk == list.get(0));
   }

}
