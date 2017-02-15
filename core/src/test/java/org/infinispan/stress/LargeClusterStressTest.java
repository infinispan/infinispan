package org.infinispan.stress;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test that we're able to start a large cluster in a single JVM.
 *
 * @author Dan Berindei
 * @since 5.3
 */
@CleanupAfterTest
@Test(groups = "stress", testName = "stress.LargeClusterStressTest")
public class LargeClusterStressTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 40;
   private static final int NUM_CACHES = 80;
   private static final int NUM_THREADS = 10;
   private static final int NUM_SEGMENTS = 1000;

   @Override
   protected void createCacheManagers() throws Throwable {
      // start the cache managers in the test itself
   }

   public void testLargeClusterStart() throws Exception {
      final Configuration distConfig = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().stateTransfer().awaitInitialTransfer(false)
//            .hash().consistentHashFactory(new TopologyAwareSyncConsistentHashFactory()).numSegments(NUM_SEGMENTS)
            .hash().consistentHashFactory(new SyncConsistentHashFactory()).numSegments(NUM_SEGMENTS)
            .build();
      final Configuration replConfig = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.REPL_SYNC)
            .clustering().hash().numSegments(NUM_SEGMENTS)
            .clustering().stateTransfer().awaitInitialTransfer(false)
            .build();

      // Start the caches (and the JGroups channels) in separate threads
      ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS, getTestThreadFactory("Worker"));
      ExecutorCompletionService<Object> completionService = new ExecutorCompletionService<Object>(executor);
      Future<Object>[] futures = new Future[NUM_NODES];
      try {
         for (int i = 0; i < NUM_NODES; i++) {
            final String nodeName = TestResourceTracker.getNameForIndex(i);
            final String machineId = "m" + (i / 2);
            futures[i] = completionService.submit(new Callable<Object>() {
               @Override
               public Object call() throws Exception {
                  GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
                  gcb.globalJmxStatistics().allowDuplicateDomains(true);
                  gcb.transport().defaultTransport().nodeName(nodeName);
//                  gcb.transport().machineId(machineId);
                  BlockingThreadPoolExecutorFactory remoteExecutorFactory = new BlockingThreadPoolExecutorFactory(
                        10, 1, 0, 60000);
                  gcb.transport().remoteCommandThreadPool().threadPoolFactory(remoteExecutorFactory);
                  BlockingThreadPoolExecutorFactory stateTransferExecutorFactory = new BlockingThreadPoolExecutorFactory(
                        4, 1, 0, 60000);
                  gcb.transport().stateTransferThreadPool().threadPoolFactory(stateTransferExecutorFactory);
                  EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build());
                  try {
                     for (int j = 0; j < NUM_CACHES/2; j++) {
                        cm.defineConfiguration("repl-cache-" + j, replConfig);
                        cm.defineConfiguration("dist-cache-" + j, distConfig);
                     }
                     for (int j = 0; j < NUM_CACHES/2; j++) {
                        Cache<Object, Object> replCache = cm.getCache("repl-cache-" + j);
                        replCache.put(cm.getAddress(), "bla");
                        Cache<Object, Object> distCache = cm.getCache("dist-cache-" + j);
                        distCache.put(cm.getAddress(), "bla");
                     }
                  } finally {
                     registerCacheManager(cm);
                  }
                  log.infof("Started cache manager %s", cm.getAddress());
                  return null;
               }
            });
         }

         for (int i = 0; i < NUM_NODES; i++) {
            completionService.take();
         }
      } finally {
         executor.shutdownNow();
      }

      log.infof("All %d cache managers started, waiting for state transfer to finish for each cache", NUM_NODES);

      for (int j = 0; j < NUM_CACHES/2; j++) {
         waitForClusterToForm("repl-cache-" + j);
         waitForClusterToForm("dist-cache-" + j);
      }
   }

   @Test(dependsOnMethods = "testLargeClusterStart")
   public void testLargeClusterStop() {
      for (int i = 0; i < NUM_NODES - 1; i++) {
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
            for (int j = 0; j < NUM_CACHES/2; j++) {
               TestingUtil.waitForStableTopology(caches("repl-cache-" + j));
               TestingUtil.waitForStableTopology(caches("dist-cache-" + j));
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
