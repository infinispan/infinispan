package org.horizon.manager;

import org.horizon.Cache;
import org.horizon.test.TestingUtil;
import org.horizon.config.Configuration;
import org.horizon.config.DuplicateCacheNameException;
import org.horizon.lifecycle.ComponentStatus;
import org.testng.annotations.Test;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerTest")
public class CacheManagerTest {
   public void testDefaultCache() {
      CacheManager cm = new DefaultCacheManager();

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
      CacheManager cm = new DefaultCacheManager();
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
      DefaultCacheManager cm = new DefaultCacheManager();
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
