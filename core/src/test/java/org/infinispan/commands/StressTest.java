package org.infinispan.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class StressTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = getClass().getName();
   final AtomicBoolean complete = new AtomicBoolean(false);
   final BlockingQueue<Throwable> exceptions = new LinkedBlockingDeque<>();
   protected ConfigurationBuilder builderUsed;

   protected Future<Void> forkRestartingThread() {
      return fork(() -> {
         TestResourceTracker.testThreadStarted(StressTest.this);
         try {
            Cache<?, ?> cacheToKill = cache(PutMapCommandStressTest.CACHE_COUNT - 1);
            while (!complete.get()) {
               Thread.sleep(1000);
               if (cacheManagers.remove(cacheToKill.getCacheManager())) {
                  log.trace("Killing cache to force rehash");
                  cacheToKill.getCacheManager().stop();
                  List<Cache<Object, Object>> caches = caches(CACHE_NAME);
                  if (caches.size() > 0) {
                     TestingUtil.blockUntilViewsReceived(60000, false, caches);
                     TestingUtil.waitForStableTopology(caches);
                  }
               } else {
                  throw new IllegalStateException("Cache Manager " + cacheToKill.getCacheManager() +
                                                        " wasn't found for some reason!");
               }

               log.trace("Adding new cache again to force rehash");
               // We should only create one so just make it the next cache manager to kill
               cacheToKill = createClusteredCaches(1, CACHE_NAME, builderUsed).get(0);
               log.trace("Added new cache again to force rehash");
            }
            return null;
         } catch (Exception e) {
            // Stop all the others as well
            complete.set(true);
            exceptions.add(e);
            throw e;
         }
      });
   }

   public void waitAndFinish(List<Future<Void>> futures, int timeout, TimeUnit timeUnit) throws Throwable {
      // If this returns means we had an issue
      Throwable e = exceptions.poll(timeout, timeUnit);
      if (e != null) {
         Throwable e2 = e;
         do {
            log.error("Exception in another thread", e2);
            e2 = exceptions.poll();
         } while (e2 != null);
         throw e;
      }

      complete.set(true);

      // Make sure they all finish properly
      for (Future future : futures) {
         future.get(1, TimeUnit.MINUTES);
      }
   }

   public <T> List<Future<Void>> forkWorkerThreads(String cacheName, int threadMultiplier, int cacheCount, T[] args, WorkerLogic<T> logic) {
      // Now we spawn off CACHE_COUNT of threads.  All but one will constantly call getAll() while another
      // will constantly be killing and adding new caches
      List<Future<Void>> futures = new ArrayList<>(threadMultiplier * (cacheCount - 1) + 1);
      for (int j = 0; j < threadMultiplier; ++j) {
         // We iterate over all but the last cache since we kill it constantly
         for (int i = 0; i < cacheCount - 1; ++i) {
            final int offset = j * (cacheCount - 1) + i;
            final Cache<Integer, Integer> cache = cache(i, cacheName);
            futures.add(fork(() -> {
               try {
                  int iteration = 0;

                  while (!complete.get()) {
                     log.tracef("Starting operation %d", iteration);
                     logic.run(cache, args[offset], iteration);
                     iteration++;
                  }
                  System.out.println(Thread.currentThread() + " finished " + iteration + " iterations!");
               } catch (Throwable e) {
                  // Stop all the others as well
                  complete.set(true);
                  exceptions.add(e);
                  throw e;
               }
               return null;
            }));
         }
      }
      return futures;
   }

   protected interface WorkerLogic<T> {
      void run(Cache<Integer, Integer> cache, T argument, int iteration) throws Exception;
   }
}
