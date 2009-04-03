package org.horizon.api.mvcc;

import org.horizon.Cache;
import org.horizon.test.fwk.TestCacheManagerFactory;
import org.horizon.config.Configuration;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.lock.LockManager;
import org.horizon.manager.CacheManager;
import org.horizon.test.SingleCacheManagerTest;
import org.horizon.test.TestingUtil;
import org.horizon.util.concurrent.locks.containers.LockContainer;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Test(groups = "functional", sequential = true, testName = "api.mvcc.LockPerEntryTest")
public class LockPerEntryTest extends SingleCacheManagerTest {
   Cache cache;

   protected CacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setUseLockStriping(false);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testLocksCleanedUp() {
      cache = cacheManager.getCache();
      cache.put("/a/b/c", "v");
      cache.put("/a/b/d", "v");
      assertNoLocks();
   }

   public void testLocksConcurrency() throws Exception {
      cache = cacheManager.getCache();
      final int NUM_THREADS = 10;
      final CountDownLatch l = new CountDownLatch(1);
      final int numLoops = 1000;
      final List<Exception> exceptions = new LinkedList<Exception>();

      Thread[] t = new Thread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++)
         t[i] = new Thread() {
            public void run() {
               try {
                  l.await();
               }
               catch (Exception e) {
                  // ignore
               }
               for (int i = 0; i < numLoops; i++) {
                  try {
                     switch (i % 2) {
                        case 0:
                           cache.put("Key" + i, "v");
                           break;
                        case 1:
                           cache.remove("Key" + i);
                           break;
                     }
                  }
                  catch (Exception e) {
                     exceptions.add(e);
                  }
               }
            }
         };

      for (Thread th : t) th.start();
      l.countDown();
      for (Thread th : t) th.join();

      if (!exceptions.isEmpty()) throw exceptions.get(0);
      assertNoLocks();
   }

   private void assertNoLocks() {
      LockManager lm = TestingUtil.extractLockManager(cache);
      LockAssert.assertNoLocks(
            lm, TestingUtil.extractComponentRegistry(cache).getComponent(InvocationContextContainer.class)
      );

      LockContainer lc = (LockContainer) TestingUtil.extractField(lm, "lockContainer");
      assert lc.size() == 0;
   }
}
