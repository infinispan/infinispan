package org.infinispan.query.statetransfer;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.helper.TestQueryHelperFactory.createCacheQuery;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;

/**
 * Base class for state transfer and query related tests
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public abstract class BaseReIndexingTest extends MultipleCacheManagersTest {

   protected Person[] persons;
   protected ConfigurationBuilder builder;

   protected abstract void configureCache(ConfigurationBuilder builder);

   @Override
   protected void createCacheManagers() {
      builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);

      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);

      configureCache(builder);

      createClusteredCaches(2, QueryTestSCI.INSTANCE, builder);
   }

   private EmbeddedCacheManager createCacheManager() {
      return addClusterEnabledCacheManager(QueryTestSCI.INSTANCE,
            builder, new TransportFlags().withMerge(true));
   }

   protected void executeSimpleQuery(Cache<String, Person> cache) {
      Person expectedPerson = persons[0];
      // Use eventually to wait for index to be updated after state transfer
      eventually(() -> {
         Query<?> cacheQuery = createCacheQuery(Person.class, cache, "blurb", "playing");
         List<?> found = cacheQuery.execute().list();
         if (found.size() != 1) {
            return false;
         }
         return expectedPerson.equals(found.get(0));
      });
   }

   protected void loadCacheEntries(Cache<String, Person> cache) {
      Person person1 = new Person();
      person1.setName("NavinSurtani");
      person1.setBlurb("Likes playing WoW");
      person1.setAge(45);

      Person person2 = new Person();
      person2.setName("BigGoat");
      person2.setBlurb("Eats grass");
      person2.setAge(30);

      Person person3 = new Person();
      person3.setName("MiniGoat");
      person3.setBlurb("Eats cheese");
      person3.setAge(35);

      Person person4 = new Person();
      person4.setName("MightyGoat");
      person4.setBlurb("Also eats grass");
      person4.setAge(66);

      persons = new Person[]{person1, person2, person3, person4};

      // Put the 3 created objects in the cache
      cache.put(person1.getName(), person1);
      cache.put(person2.getName(), person2);
      cache.put(person3.getName(), person3);
      cache.put(person4.getName(), person4);
   }

   protected void addNodeCheckingContentsAndQuery() {
      withCacheManager(new CacheManagerCallable(createCacheManager()) {
         @Override
         public void call() {
            // New node joining
            Cache<String, Person> newCache = cm.getCache();
            TestingUtil.waitForNoRebalance(caches().get(0), caches().get(1), newCache);

            // Verify state transfer
            int size = newCache.size();
            assertEquals(4, size);
            for (int i = 0; i < size; i++)
               assertEquals(persons[i], newCache.get(persons[i].getName()));

            // Repeat query on new node
            executeSimpleQuery(newCache);
         }
      });
   }

}
