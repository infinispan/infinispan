package org.infinispan.query.tx;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.test.TestingUtil.withTx;

import java.util.concurrent.Callable;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test to make sure indexLocalOnly=false behaves the same with or without
 * transactions enabled.
 * See also ISPN-2467 and subclasses.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.tx.NonLocalIndexingTest")
public class NonLocalIndexingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class);
      createClusteredCaches(2, QueryTestSCI.INSTANCE, builder);
   }

   //Extension point to override configuration
   protected boolean transactionsEnabled() {
      return false;
   }

   public void testQueryAfterAddingNewNode() throws Exception {
      store("Astronaut", new Person("Astronaut", "is asking his timezone", 32), cache(0));
      assertFind("timezone", 1);
      assertFind("asking", 1);
      assertFind("cat", 0);
      store("Webdeveloper", new Person("Webdeveloper", "is confused by the timezone concept", 32), cache(1));
      assertFind("cat", 0);
      assertFind("timezone", 2);
      //We're using the name as a key so this is an update in practice of the Astronaut record:
      store("Astronaut", new Person("Astronaut", "thinks about his cat", 32), cache(1));
      assertFind("cat", 1);
      assertFind("timezone", 1);
      store("Astronaut", new AnotherGrassEater("Astronaut", "is having a hard time to find grass"), cache(1));
      //replacing same key with a new type:
      assertFind("cat", 0);
      assertFind("grass", 0);
   }

   private void assertFind(String keyword, int expectedCount) {
      assertFind(cache(0), keyword, expectedCount);
      assertFind(cache(1), keyword, expectedCount);
   }

   private static void assertFind(Cache cache, String keyword, int expectedCount) {
      String q = String.format("FROM %s WHERE blurb:'%s'", Person.class.getName(), keyword);
      Query<Object> cacheQuery = Search.getQueryFactory(cache).create(q);
      long resultSize = cacheQuery.execute().hitCount().orElse(-1);
      Assert.assertEquals(resultSize, expectedCount);
   }

   private void store(final String key, final Object value, final Cache<Object, Object> cache) throws Exception {
      Callable<Void> callable = () -> {
         cache.put(key, value);
         return null;
      };

      if (transactionsEnabled()) {
         withTx(cache.getAdvancedCache().getTransactionManager(), callable);
      } else {
         callable.call();
      }
   }

}
