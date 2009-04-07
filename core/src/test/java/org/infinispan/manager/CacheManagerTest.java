package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.TestingUtil;
import org.infinispan.config.Configuration;
import org.infinispan.config.DuplicateCacheNameException;
import org.infinispan.lifecycle.ComponentStatus;
import org.testng.annotations.Test;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerTest")
public class CacheManagerTest {
   public void testDefaultCache() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();

      try {
         assert cm.getCache().getStatus() == ComponentStatus.RUNNING;
         assert cm.getCache().getName().equals(DefaultCacheManager.DEFAULT_CACHE_NAME);

         try {
            cm.defineCache(DefaultCacheManager.DEFAULT_CACHE_NAME, new Configuration());
            assert false : "Should fail";
         }
         catch (IllegalArgumentException e) {
            // ok
            assert true : "Allowed";
         }
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testClashingNames() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Configuration c = new Configuration();

         cm.defineCache("aCache", c);
         try {
            cm.defineCache("aCache", c);
            assert false : "Should fail";
         }
         catch (DuplicateCacheNameException cnee) {
            // expected
            assert true : "Expected";
         }
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testStartAndStop() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Cache c1 = cm.getCache("cache1");
         Cache c2 = cm.getCache("cache2");
         Cache c3 = cm.getCache("cache3");

         assert c1.getStatus() == ComponentStatus.RUNNING;
         assert c2.getStatus() == ComponentStatus.RUNNING;
         assert c3.getStatus() == ComponentStatus.RUNNING;

         cm.stop();

         assert c1.getStatus() == ComponentStatus.TERMINATED;
         assert c2.getStatus() == ComponentStatus.TERMINATED;
         assert c3.getStatus() == ComponentStatus.TERMINATED;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
