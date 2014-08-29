package org.infinispan.query.blackbox;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * Tests for testing clustered queries functionality on multiple cache instances
 * (In these tests we have two caches in each CacheManager)
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredQueryMultipleCachesTest")
public class ClusteredQueryMultipleCachesTest extends ClusteredQueryTest {

   Cache<String, Person> cacheBMachine1, cacheBMachine2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(getCacheMode(), false);
      cacheCfg.indexing().index(Index.LOCAL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cacheCfg);
      String[] cacheNames = { "cacheA", "cacheB" };
      List<List<Cache<String, Person>>> caches = createClusteredCaches(2, cacheCfg, cacheNames);
      cacheAMachine1 = caches.get(0).get(0);
      cacheAMachine2 = caches.get(1).get(0);
      cacheBMachine1 = caches.get(0).get(1);
      cacheBMachine2 = caches.get(1).get(1);
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
