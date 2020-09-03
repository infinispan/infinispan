package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.Index.PRIMARY_OWNER;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.blackbox.ExpirationLocalTest")
public class ExpirationLocalTest extends SingleCacheManagerTest {

   private final ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);

      builder.indexing().index(PRIMARY_OWNER).addIndexedEntities(Person.class)
            .addProperty("default.directory_provider", "ram");
      cacheManager = TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, builder);
      TestingUtil.replaceComponent(cacheManager, TimeService.class, timeService, true);
      return cacheManager;
   }

   void expireEntries() {
      timeService.advance(TimeUnit.MINUTES.toMillis(2));
      cache.getAdvancedCache().getExpirationManager().processExpiration();
   }

   private void loadData() {
      for (int i = 0; i < 10; i++) {
         cache.put(String.valueOf(i), new Person("name" + i, "blurb" + i, 22));
      }
   }

   private int indexSize() {
      return Search.getQueryFactory(cache).create(String.format("FROM %s", Person.class.getName())).getResultSize();
   }

   @Test
   public void shouldRemoveExpiredDataFromIndexes() {
      loadData();

      assertEquals(10, cache.size());
      assertEquals(10, indexSize());

      // Add mortal entry
      cache.put("11", new Person("p", "b", 10), 1, TimeUnit.MINUTES);

      assertEquals(11, cache.size());
      assertEquals(11, indexSize());

      expireEntries();

      assertEquals(10, cache.size());
      assertEquals(10, indexSize());
   }

}
