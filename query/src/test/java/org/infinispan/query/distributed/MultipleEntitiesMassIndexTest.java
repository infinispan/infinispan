package org.infinispan.query.distributed;

import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.context.Flag;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * Test the MassIndexer in a configuration of multiple entity types per cache.
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.MultipleEntitiesMassIndexTest")
public class MultipleEntitiesMassIndexTest extends DistributedMassIndexingTest {

   protected void createCacheManagers() throws Throwable {
      // Person goes to RAM, Cars goes to Infinispan
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg
            .indexing()
            .index(Index.ALL)
            .addIndexedEntity(Car.class)
            .addIndexedEntity(Person.class)
            .addProperty("hibernate.search.person.directory_provider", "ram")
            .addProperty("hibernate.search.car.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      List<Cache<String, Car>> cacheList = createClusteredCaches(2, cacheCfg);

      waitForClusterToForm();

      for (Cache cache : cacheList) {
         caches.add(cache);
      }
   }

   @Override
   @SuppressWarnings("unchecked")
   public void testReindexing() throws Exception {
      caches.get(0).put(key("C1"), new Car("megane", "white", 300));
      caches.get(1).put(key("P1"), new Person("james", "blurb", 23));
      caches.get(1).put(key("P2"), new Person("tony", "blurb", 28));
      caches.get(1).put(key("P3"), new Person("chris", "blurb", 26));
      caches.get(1).put(key("P4"), new Person("iker", "blurb", 23));
      caches.get(1).put(key("P5"), new Person("sergio", "blurb", 29));

      checkIndex(5, Person.class);
      checkIndex(1, Car.class);
      checkIndex(1, "make", "megane", Car.class);
      checkIndex(1, "name", "james", Person.class);

      caches.get(1).put(key("C2"), new Car("megane", "blue", 300));
      checkIndex(2, "make", "megane", Car.class);

      //add an entry without indexing it:
      caches.get(1).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("C3"), new Car("megane", "blue", 300));
      checkIndex(2, "make", "megane", Car.class);

      //re-sync datacontainer with indexes:
      rebuildIndexes();
      checkIndex(5, Person.class);
      checkIndex(3, Car.class);
      checkIndex(3, "make", "megane", Car.class);
      checkIndex(1, "name", "tony", Person.class);

      //verify we cleanup old stale index values by removing the data but avoid touching the index
      caches.get(1).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).remove(key("C2"));
      caches.get(1).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).remove(key("P3"));
      assertNull(caches.get(1).get(key("P3")));
      assertNull(caches.get(1).get(key("C2")));
      checkIndex(3, "make", "megane", Car.class);
      checkIndex(5, Person.class);

      //re-sync
      rebuildIndexes();
      checkIndex(2, Car.class);
      checkIndex(2, "make", "megane", Car.class);
      checkIndex(4, Person.class);
   }

   protected void checkIndex(int expectedCount, String fieldName, String fieldValue, Class<?> entity) throws ParseException {
      Query q = new QueryParser(fieldName, new StandardAnalyzer()).parse(fieldName + ":" + fieldValue);
      checkIndex(expectedCount, q, entity);
   }


   protected void checkIndex(int expectedCount, Class<?> entity) throws ParseException {
      checkIndex(expectedCount, new MatchAllDocsQuery(), entity);
   }

   private void checkIndex(int expectedCount, Query luceneQuery, Class<?> entity) throws ParseException {
      for (Cache cache : caches) {
         StaticTestingErrorHandler.assertAllGood(cache);
         SearchManager searchManager = Search.getSearchManager(cache);
         CacheQuery<?> cacheQuery = searchManager.getQuery(luceneQuery, entity);
         assertEquals(expectedCount, cacheQuery.getResultSize());
      }
   }
}
