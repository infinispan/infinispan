package org.infinispan.persistence.leveldb;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

@Test(groups = "unit", testName = "persistence.leveldb.JavaLevelDBStoreFunctionalTest")
public class JavaLevelDBStoreFunctionalTest extends LevelDBStoreFunctionalTest {

   public static final int EXPIRATION_TIMEOUT = 3000;

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder loaders, boolean preload) {
      super.createStoreBuilder(loaders).implementationType(LevelDBStoreConfiguration.ImplementationType.JAVA).preload(preload);
      return loaders;
   }

   public void testEntrySetAfterExpiryWithStore() throws Exception {

      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), true);
      CacheContainer local = TestCacheManagerFactory.createCacheManager(cb);
      try {
         Cache<String, String> cache = local.getCache();
         cacheNames.add(cache.getName());
         cache.start();
         cache.clear();

         assertEquals(cache.entrySet().size(), 0);

         Map dataIn = new HashMap();
         dataIn.put(1, 1);
         dataIn.put(2, 2);

         cache.putAll(dataIn, EXPIRATION_TIMEOUT, TimeUnit.MILLISECONDS);

         Thread.sleep(EXPIRATION_TIMEOUT + 1000);
         assertEquals(cache.entrySet().size(), 0);
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }

}
