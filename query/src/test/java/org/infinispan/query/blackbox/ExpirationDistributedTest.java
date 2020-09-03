package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.Index.PRIMARY_OWNER;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.blackbox.ExpirationTest")
public class ExpirationDistributedTest extends MultipleCacheManagersTest {

   private Cache<String, Person> cache;
   private final ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.indexing().index(PRIMARY_OWNER)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
            .addIndexedEntities(Person.class);

      createClusteredCaches(2, QueryTestSCI.INSTANCE, builder);
      cacheManagers.forEach(cm -> TestingUtil.replaceComponent(cm, TimeService.class, timeService, true));
      cache = cache(0);
   }

   void expireEntries() {
      timeService.advance(TimeUnit.MINUTES.toMillis(2));
      cache.getAdvancedCache().getExpirationManager().processExpiration();
   }

   private void loadData(int startKey, int count) {
      String[] names = new String[]{"John", "Mary", "Luc"};
      String[] blurbs = new String[]{"Listener", "Talker", "Watcher"};
      for (int i = startKey; i < startKey + count; i++) {
         int index = i % 3;
         cache.put(String.valueOf(i), new Person(names[index], blurbs[index], 22));
      }
   }

   @Test
   public void shouldRemoveExpiredDataFromIndexes() {
      // Load 10 immortal entries
      loadData(1, 10);

      QueryFactory queryFactory = Search.getQueryFactory(cache);

      String entityName = Person.class.getName();
      Supplier<Query> queryAll = () -> queryFactory.create(String.format("FROM %s", entityName));

      assertEquals(10, cache.size());
      assertEquals(10, queryAll.get().getResultSize());

      // Add mortal entry
      cache.put("11", new Person("p", "b", 10), 1, TimeUnit.MINUTES);

      assertEquals(11, cache.size());
      assertEquals(11, queryAll.get().getResultSize());

      expireEntries();

      assertEquals(10, cache.size());
      assertEquals(10, queryAll.get().getResultSize());
   }


}
