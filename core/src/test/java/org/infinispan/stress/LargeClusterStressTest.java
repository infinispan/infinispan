package org.infinispan.stress;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.INVALIDATION_SYNC;
import static org.infinispan.configuration.cache.CacheMode.LOCAL;
import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.infinispan.Cache;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test that we're able to start a large cluster in a single JVM.
 *
 * @author Dan Berindei
 * @since 5.3
 */
@CleanupAfterTest
@Test(groups = "stress", testName = "stress.LargeClusterStressTest", timeOut = 15*60*1000)
public class LargeClusterStressTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 40;
   private static final int NUM_CACHES = 20;
   private static final int NUM_THREADS = NUM_NODES;
   private static final int NUM_SEGMENTS = 256;

   private static final EnumSet<CacheMode> cacheModes = EnumSet.of(CacheMode.LOCAL, CacheMode.INVALIDATION_SYNC, DIST_SYNC, REPL_SYNC);

   @Override
   protected void createCacheManagers() throws Throwable {
      // start the cache managers in the test itself
   }

   public void testLargeClusterStart() throws Exception {
      Map<CacheMode, Configuration> configurations = new HashMap<>();
      configurations.put(DIST_SYNC, new ConfigurationBuilder()
            .clustering().cacheMode(DIST_SYNC)
            .clustering().stateTransfer().awaitInitialTransfer(false)
            .clustering().hash().numSegments(NUM_SEGMENTS)
            .build());
      configurations.put(REPL_SYNC, new ConfigurationBuilder()
            .clustering().cacheMode(REPL_SYNC)
            .clustering().hash().numSegments(NUM_SEGMENTS)
            .clustering().stateTransfer().awaitInitialTransfer(false)
            .build());
      configurations.put(INVALIDATION_SYNC, new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.INVALIDATION_SYNC)
            .build());
      configurations.put(LOCAL, new ConfigurationBuilder()
            .clustering().cacheMode(LOCAL)
            .build());

      // Start the caches (and the JGroups channels) in separate threads
      ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS, getTestThreadFactory("Worker"));
      ExecutorCompletionService<Object> completionService = new ExecutorCompletionService<>(executor);
      try {
         for (int i = 0; i < NUM_NODES; i++) {
            final String nodeName = TestResourceTracker.getNameForIndex(i);
//            final String machineId = "m" + (i / 2);
            completionService.submit(() -> {
               GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
               gcb.transport().defaultTransport().nodeName(nodeName);
//                  gcb.transport().machineId(machineId);
               EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build());
               try {
                  for (int j = 0; j < NUM_CACHES; j++) {
                     for (CacheMode mode : cacheModes) {
                        cm.defineConfiguration(cacheName(mode, j), configurations.get(mode));
                     }
                  }
                  for (int j = 0; j < NUM_CACHES; j++) {
                     for (CacheMode mode : cacheModes) {
                        Cache<Object, Object> cache = cm.getCache(cacheName(mode, j));
                        cache.put(cm.getAddress(), "bla");
                     }
                  }
               } finally {
                  registerCacheManager(cm);
               }
               log.infof("Started cache manager %s", cm.getAddress());
               return null;
            });
         }

         for (int i = 0; i < NUM_NODES; i++) {
            completionService.take();
         }
      } finally {
         executor.shutdownNow();
      }

      log.infof("All %d cache managers started, waiting for state transfer to finish for each cache", NUM_NODES);

      for (int j = 0; j < NUM_CACHES; j++) {
         for (CacheMode mode : cacheModes) {
            if (mode.isClustered()) {
               waitForClusterToForm(cacheName(mode, j));
            }
         }
      }
   }

   private String cacheName(CacheMode mode, int index) {
      return mode.toString().toLowerCase() + "-cache-" + index;
   }

   @Test(dependsOnMethods = "testLargeClusterStart")
   public void testLargeClusterStop() {
      for (int i = 0; i < NUM_NODES; i++) {
         int killIndex = -1;
         for (int j = 0; j < cacheManagers.size(); j++) {
            if (address(j).equals(manager(0).getCoordinator())) {
               killIndex = j;
               break;
            }
         }

         log.debugf("Killing coordinator %s", address(killIndex));
         manager(killIndex).stop();
         cacheManagers.remove(killIndex);
         if (cacheManagers.size() > 0) {
            TestingUtil.blockUntilViewsReceived(60000, false, cacheManagers);
            for (int j = 0; j < NUM_CACHES; j++) {
               for (CacheMode mode : cacheModes) {
                  if (mode.isClustered()) {
                     waitForNoRebalance(caches(cacheName(mode, j)));
                  }
               }
            }
         }
      }
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      // Do nothing
   }
}
