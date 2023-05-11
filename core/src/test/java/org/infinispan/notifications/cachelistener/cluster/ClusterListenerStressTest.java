package org.infinispan.notifications.cachelistener.cluster;

import static org.testng.Assert.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

/**
 * Stress test that simultates multiple writers to different cache nodes to verify cluster listener is notified
 * properly.
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "stress", testName = "notifications.cachelistener.cluster.ClusterListenerStressTest", timeOut = 15*60*1000)
@InCacheMode({ CacheMode.DIST_SYNC })
public class ClusterListenerStressTest extends MultipleCacheManagersTest {
   protected final static String CACHE_NAME = "cluster-listener";
   protected final static String KEY = "ClusterListenerStressTestKey";
   private static final int NUM_NODES = 3;

   protected ConfigurationBuilder builderUsed;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration distConfig = getDefaultClusteredCacheConfig(cacheMode, false).build();
      for (int i = 0; i < NUM_NODES; i++) {
         GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
         gcb.transport().defaultTransport().nodeName(TestResourceTracker.getNameForIndex(i));
         BlockingThreadPoolExecutorFactory remoteExecutorFactory = new BlockingThreadPoolExecutorFactory(
               10, 1, 0, 60000);
         gcb.transport().remoteCommandThreadPool().threadPoolFactory(remoteExecutorFactory);
         EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build());
         registerCacheManager(cm);
         cm.defineConfiguration(CACHE_NAME, distConfig);
         log.infof("Started cache manager %s", cm.getAddress());
      }
      waitForClusterToForm(CACHE_NAME);
   }

   @Listener(clustered = true)
   private static class ClusterListenerAggregator {
      AtomicInteger creationCount = new AtomicInteger();
      AtomicInteger modifyCount = new AtomicInteger();
      AtomicInteger removalCount = new AtomicInteger();

      @CacheEntryCreated
      public void listenForModifications(CacheEntryEvent<String, Integer> event) {
         creationCount.incrementAndGet();
      }

      @CacheEntryModified
      public void modified(CacheEntryEvent<String, Integer> event) {
         modifyCount.incrementAndGet();
      }

      @CacheEntryRemoved
      public void removed(CacheEntryEvent<String, Integer> event) {
         removalCount.incrementAndGet();
      }
   }

   private static class CreateModifyRemovals {
      private final int creationCount;
      private final int modifyCount;
      private final int removalCount;

      public CreateModifyRemovals(int creationCount, int modifyCount, int removalCount) {
         this.creationCount = creationCount;
         this.modifyCount = modifyCount;
         this.removalCount = removalCount;
      }
   }

   @Test
   public void runStressTestMultipleWriters() throws ExecutionException, InterruptedException {
      Cache<String, Integer> cache0 = cache(0, CACHE_NAME);
      Cache<String, Integer> cache1 = cache(1, CACHE_NAME);
      Cache<String, Integer> cache2 = cache(2, CACHE_NAME);

      ClusterListenerAggregator listener = new ClusterListenerAggregator();
      cache0.addListener(listener);
      cache0.addListener(listener);
      cache0.addListener(listener);

      cache1.addListener(listener);
      cache1.addListener(listener);

      cache2.addListener(listener);

      long begin = System.currentTimeMillis();

      int threadCount = 10;
      final CountDownLatch latch = new CountDownLatch(threadCount);
      Callable<CreateModifyRemovals> callable = new Callable<CreateModifyRemovals>() {

         @Override
         public CreateModifyRemovals call() throws Exception {
            latch.countDown();
            latch.await();
            int creationCount = 0;
            int modifyCount = 0;
            int removalCount = 0;
            for (int i = 0; i < 1000; i++) {
               int random = ThreadLocalRandom.current().nextInt(0, 23);
               boolean key = (random & 1) == 1;
               int cache = random / 8;
               int operation = random & 3;
               Cache<String, Integer> cacheToUse = cache(cache, CACHE_NAME);
               String keyToUse = key ? KEY : KEY + "2";
               // 0 - regular put operation (detects if create modify)
               // 1 - remove operation
               // 2 - conditional replace/putIfAbsent
               // 3 - conditional remove
               switch (operation) {
                  case 0:
                     Integer prevValue = cacheToUse.put(keyToUse, i);
                     if (prevValue != null) {
                        modifyCount++;
                     } else {
                        creationCount++;
                     }
                     break;
                  case 1:
                     cacheToUse.remove(keyToUse);
                     removalCount++;
                     break;
                  case 2:
                     prevValue = cacheToUse.get(keyToUse);
                     if (prevValue != null) {
                        if (cacheToUse.replace(keyToUse, prevValue, i)) {
                           modifyCount++;
                        }
                     } else {
                        if (cacheToUse.putIfAbsent(keyToUse, i) == null) {
                           creationCount++;
                        }
                     }
                     break;
                  case 3:
                     prevValue = cacheToUse.get(keyToUse);
                     if (prevValue != null) {
                        cacheToUse.remove(keyToUse, prevValue);
                        // We always notify on a removal even if it didn't remove it!
                        removalCount++;
                     }
                     break;
                  default:
                     throw new IllegalArgumentException("Unsupported case!, provided " + operation);
               }

            }
            return new CreateModifyRemovals(creationCount, modifyCount, removalCount);
         }
      };
      Future<CreateModifyRemovals>[] futures = new Future[threadCount];
      for (int i = 0; i < threadCount; ++i) {
         futures[i] = fork(callable);
      }

      int creationCount = 0;
      int modifyCount = 0;
      int removalCount = 0;
      for (Future<CreateModifyRemovals> future : futures) {
         CreateModifyRemovals cmr = future.get();

         creationCount += cmr.creationCount;
         modifyCount += cmr.modifyCount;
         removalCount += cmr.removalCount;
      }

      int listenerCount = 6;

      assertEquals(listener.creationCount.get(), creationCount * listenerCount);
      assertEquals(listener.modifyCount.get(), modifyCount * listenerCount);
      assertEquals(listener.removalCount.get(), removalCount * listenerCount);

      System.out.println("Took " + (System.currentTimeMillis() - begin) + " milliseconds");
   }
}
