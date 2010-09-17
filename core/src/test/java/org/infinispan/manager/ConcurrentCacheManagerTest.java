package org.infinispan.manager;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Test that verifies that CacheContainer.getCache() can be called concurrently.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "ConcurrentCacheManagerTest")
public class ConcurrentCacheManagerTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return new DefaultCacheManager();
   }

   public void testConcurrentGetCacheCalls() throws Exception {
      int numThreads = 25;
      final CyclicBarrier barrier = new CyclicBarrier(numThreads +1);
      List<Future<Void>> futures = new ArrayList<Future<Void>>(numThreads);
      ExecutorService executorService = Executors.newCachedThreadPool();
      for (int i = 0; i < numThreads; i++) {
         log.debug("Schedule execution");
         Future<Void> future = executorService.submit(new Callable<Void>(){
            @Override
            public Void call() throws Exception {
               try {
                  barrier.await();
                  cacheManager.getCache("blahblah").put("a", "b");
                  return null;
               } finally {
                  log.debug("Wait for all execution paths to finish");
                  barrier.await();
               }
            }
         });
         futures.add(future);
      }
      barrier.await(); // wait for all threads to be ready
      barrier.await(); // wait for all threads to finish

      log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
      for (Future<Void> future : futures) future.get();
   }
}
