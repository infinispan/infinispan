package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Tests {@link Flag#SKIP_LOCKING} logic
 *
 * @author Galder Zamarre�o
 * @since 4.1
 */
@Test(groups = "functional", testName = "api.SkipLockingTest")
public class SkipLockingTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(false);
   }

   public void testSkipLockingAfterPutWithoutTm(Method m) {
      String name = m.getName();
      AdvancedCache advancedCache = cacheManager.getCache().getAdvancedCache();
      advancedCache.put("k-" + name, "v-" + name);
      advancedCache.withFlags(Flag.SKIP_LOCKING).put("k-" + name, "v2-" + name);
   }

   public void testSkipLockingAfterPutWithTm(Method m) {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(true);
      try {
         AdvancedCache advancedCache = cacheManager.getCache().getAdvancedCache();
         String name = m.getName();
         advancedCache.put("k-" + name, "v-" + name);
         advancedCache.withFlags(Flag.SKIP_LOCKING).put("k-" + name, "v2-" + name);
      } finally {
         cacheManager.stop();
      }
   }

}
