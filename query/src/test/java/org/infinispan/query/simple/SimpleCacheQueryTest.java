package org.infinispan.query.simple;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.embedded.SingleClassDSLQueryTest;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.query.SimpleCacheQueryTest")
public class SimpleCacheQueryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.simpleCache(true);

      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*Simple caches do not support queries.*")
   public void test() {
      cache.put("person1", new SingleClassDSLQueryTest.Person("William", "Shakespeare", 50, "ZZ3141592", "M"));
      cache.query("FROM " + SingleClassDSLQueryTest.Person.class.getName());
   }
}
