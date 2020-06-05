package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.Search;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Tests for testing clustered queries functionality on multiple cache instances
 * (In these tests we have two caches in each CacheManager)
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredQueryMultipleCachesTest")
public class ClusteredQueryMultipleCachesTest extends ClusteredQueryTest {

   Cache<String, Person> cacheBMachine1, cacheBMachine2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(getCacheMode(), false);
      cacheCfg.indexing().enable()
            .addIndexedEntity(Person.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg, new TransportFlags(), "cacheA", "cacheB");
      cacheAMachine1 = manager(0).getCache("cacheA");
      cacheAMachine2 = manager(1).getCache("cacheA");
      cacheBMachine1 = manager(0).getCache("cacheB");
      cacheBMachine2 = manager(1).getCache("cacheB");
      queryFactory1 = Search.getQueryFactory(cacheAMachine1);
      queryFactory2 = Search.getQueryFactory(cacheAMachine2);
      populateCache();
   }

   @Override
   protected void prepareTestData() {
      super.prepareTestData();

      Person person5 = new Person();
      person5.setName("People In Another Cache");
      person5.setBlurb("Also eats grass");
      person5.setAge(5);

      cacheBMachine2.put("anotherNewOne", person5);
   }

}
