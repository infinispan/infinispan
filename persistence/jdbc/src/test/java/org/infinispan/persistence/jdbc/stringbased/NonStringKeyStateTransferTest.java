package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.NonStringKeyStateTransferTest")
public class NonStringKeyStateTransferTest extends AbstractCacheTest {

   public void testReplicatedStateTransfer() {
      EmbeddedCacheManager cm1 = null, cm2 = null;
      try {
         cm1 = createCacheManager(true, CacheMode.REPL_SYNC);
         Cache<Person, String> c1 = cm1.getCache();
         Person mircea = new Person("mircea");
         Person mircea2 = new Person("mircea2");

         c1.put(mircea, "mircea");
         c1.put(mircea2, "mircea2");

         cm2 = createCacheManager(true, CacheMode.REPL_SYNC);
         Cache<Person, String> c2 = cm2.getCache();
         assertEquals("mircea", c2.get(mircea));
         assertEquals("mircea2", c2.get(mircea2));
         c2.get(mircea2);
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testDistributedStateTransfer() {
      EmbeddedCacheManager cm1 = null, cm2 = null;
      try {
         cm1 = createCacheManager(false, CacheMode.DIST_SYNC);
         Cache<Person, String> c1 = cm1.getCache();

         for (int i = 0; i < 100; i++) {
            Person mircea = new Person("mircea"+i);
            c1.put(mircea, "mircea"+i);
         }

         cm2 = createCacheManager(false, CacheMode.DIST_SYNC);
         Cache<Person, String> c2 = cm2.getCache();
         assert c2.size() > 0;
         for (Object key: c2.getAdvancedCache().getDataContainer().keySet()) {
            assert key instanceof Person: "expected key to be person but obtained " + key;
         }
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testDistributedAndNoTwoWay() {
      ConfigurationBuilder config = NonStringKeyPreloadTest.createCacheStoreConfig(TwoWayPersonKey2StringMapper.class.getName(), false);
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      withCacheManager(() -> TestCacheManagerFactory.createClusteredCacheManager(TestDataSCI.INSTANCE, config), EmbeddedCacheManager::getCache);
   }

   private EmbeddedCacheManager createCacheManager(boolean preload, CacheMode cacheMode) {
      ConfigurationBuilder config = NonStringKeyPreloadTest.createCacheStoreConfig(TwoWayPersonKey2StringMapper.class.getName(), preload);
      config.clustering().cacheMode(cacheMode);
      return TestCacheManagerFactory.createClusteredCacheManager(TestDataSCI.INSTANCE, config);
   }
}
