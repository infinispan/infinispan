package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;

/**
 * Add test that exercises concurrent behaviour of both
 * {@link org.infinispan.interceptors.IsMarshallableInterceptor}
 * and marshalling layer.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "marshall.ConcurrentMarshallerTest")
public class ConcurrentMarshallerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(
            CacheMode.REPL_ASYNC, false);
      builder.clustering().async().useReplQueue(true);
      createClusteredCaches(2, "concurrentMarshaller", builder);
   }

   public void test000() throws Exception {
      final Cache cache1 = cache(0,"concurrentMarshaller");

      int nbWriters = 10;
      final CyclicBarrier barrier = new CyclicBarrier(nbWriters + 1);
      List<Future<Void>> futures = new ArrayList<Future<Void>>(nbWriters);

      for (int i = 0; i < nbWriters; i++) {
         log.debug("Schedule execution");
         Future<Void> future = fork(
               new CacheUpdater(barrier, cache1));
         futures.add(future);
      }
      barrier.await(); // wait for all threads to be ready
      barrier.await(); // wait for all threads to finish

      log.debug("Threads finished, shutdown executor and check for exceptions");
      for (Future<Void> future : futures) future.get();
   }

   static class CacheUpdater implements Callable<Void> {

      static final Log log = LogFactory.getLog(CacheUpdater.class);

      CyclicBarrier barrier;
      Cache cache;

      CacheUpdater(CyclicBarrier barrier, Cache cache) {
         this.barrier = barrier;
         this.cache = cache;
      }

      @Override
      public Void call() throws Exception {
         log.debug("Wait for all executions paths to be ready");
         barrier.await();

         try {
            for (int i = 0; i < 10; i ++) {
               String decimal = Integer.toString(i);
               byte[] key = ("key-" + Thread.currentThread().getName() + decimal).getBytes();
               byte[] value = ("value-" + Thread.currentThread().getName() + decimal).getBytes();
               cache.put(key, value);
            }
            return null;
         } finally {
            log.debug("Wait for all execution paths to finish");
            barrier.await();
         }
      }

   }

}
