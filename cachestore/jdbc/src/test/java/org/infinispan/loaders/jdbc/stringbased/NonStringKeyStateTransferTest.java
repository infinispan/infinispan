package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "jdbc.stringbased.NonStringKeyStateTransferTest")
public class NonStringKeyStateTransferTest extends AbstractCacheTest {

   public void testReplicatedStateTransfer() {
      EmbeddedCacheManager cm1 = null, cm2 = null;
      try {
         Configuration conf = NonStringKeyPreloadTest.createCacheStoreConfig(TwoWayPersonKey2StringMapper.class.getName(), false, true);
         conf.setCacheMode(Configuration.CacheMode.REPL_SYNC);

         cm1 = TestCacheManagerFactory.createClusteredCacheManager(conf);
         Cache<Person, String> c1 = cm1.getCache();
         Person mircea = new Person("markus", "mircea", 30);
         Person mircea2 = new Person("markus2", "mircea2", 30);

         c1.put(mircea, "mircea");
         c1.put(mircea2, "mircea2");

         cm2 = TestCacheManagerFactory.createClusteredCacheManager(conf);
         Cache c2 = cm2.getCache();
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
         Configuration conf = NonStringKeyPreloadTest.createCacheStoreConfig(TwoWayPersonKey2StringMapper.class.getName(), false, false);
         conf.setCacheMode(Configuration.CacheMode.DIST_SYNC);

         cm1 = TestCacheManagerFactory.createClusteredCacheManager(conf);
         Cache<Person, String> c1 = cm1.getCache();

         for (int i = 0; i < 100; i++) {
            Person mircea = new Person("markus" +i , "mircea"+i, 30);
            c1.put(mircea, "mircea"+i);
         }

         cm2 = TestCacheManagerFactory.createClusteredCacheManager(conf);
         Cache c2 = cm2.getCache();
         assert c2.size() > 0;
         for (Object key: c2.getAdvancedCache().getDataContainer().keySet()) {
            assert key instanceof Person: "expected key to be person but obtained " + key;
         }
         
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testDistributedAndNoTwoWay() {
      EmbeddedCacheManager cm1;

      Configuration conf = NonStringKeyPreloadTest.createCacheStoreConfig(TwoWayPersonKey2StringMapper.class.getName(), false, false);
      conf.setCacheMode(Configuration.CacheMode.DIST_SYNC);

      cm1 = TestCacheManagerFactory.createClusteredCacheManager(conf);
      try {
         cm1.getCache();
      } finally {
         TestingUtil.killCacheManagers(cm1);
      }

   }
}
