package org.infinispan.query.distributed;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
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
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Car.class)
            .addIndexedEntity(Person.class)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());

      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);

      waitForClusterToForm();
   }

   @Override
   public void testReindexing() throws Exception {
      cache(0).put(key("C1"), new Car("megane", "white", 300));
      cache(1).put(key("P1"), new Person("james", "blurb", 23));
      cache(1).put(key("P2"), new Person("tony", "blurb", 28));
      cache(1).put(key("P3"), new Person("chris", "blurb", 26));
      cache(1).put(key("P4"), new Person("iker", "blurb", 23));
      cache(1).put(key("P5"), new Person("sergio", "blurb", 29));

      checkIndex(5, Person.class);
      checkIndex(1, Car.class);
      checkIndex(1, "make", "megane", Car.class);
      checkIndex(1, "name", "james", Person.class);

      cache(1).put(key("C2"), new Car("megane", "blue", 300));
      checkIndex(2, "make", "megane", Car.class);

      //add an entry without indexing it:
      cache(1).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("C3"), new Car("megane", "blue", 300));
      checkIndex(2, "make", "megane", Car.class);

      //re-sync datacontainer with indexes:
      rebuildIndexes();
      checkIndex(5, Person.class);
      checkIndex(3, Car.class);
      checkIndex(3, "make", "megane", Car.class);
      checkIndex(1, "name", "tony", Person.class);

      //verify we cleanup old stale index values by removing the data but avoid touching the index
      cache(1).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).remove(key("C2"));
      cache(1).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).remove(key("P3"));
      assertNull(cache(1).get(key("P3")));
      assertNull(cache(1).get(key("C2")));
      checkIndex(3, "make", "megane", Car.class);
      checkIndex(5, Person.class);

      //re-sync
      rebuildIndexes();
      checkIndex(2, Car.class);
      checkIndex(2, "make", "megane", Car.class);
      checkIndex(4, Person.class);
   }

   private void checkIndex(int expectedCount, String fieldName, String fieldValue, Class<?> entity) {
      String q = String.format("FROM %s where %s:'%s'", entity.getName(), fieldName, fieldValue);
      checkIndex(expectedCount, q, entity);
   }

   private void checkIndex(int expectedCount, Class<?> entity) {
      checkIndex(expectedCount, "FROM " + entity.getName(), entity);
   }

   private void checkIndex(int expectedCount, String q, Class<?> entity) {
      for (Cache<?, ?> cache : caches()) {
         StaticTestingErrorHandler.assertAllGood(cache);
         QueryFactory searchManager = Search.getQueryFactory(cache);
         Query cacheQuery = searchManager.create(q);
         assertEquals(expectedCount, cacheQuery.execute().hitCount().orElse(-1));
      }
   }
}
