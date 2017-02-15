package org.infinispan.stress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
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
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.util.concurrent.TimeoutException;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test that we're able to start a cluster with lots of caches in a single JVM.
 *
 * @author Dan Berindei
 * @since 7.2
 */
@CleanupAfterTest
@Test(groups = "stress", testName = "stress.LargeCluster2StressTest")
public class LargeCluster2StressTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 10;
   private static final int NUM_CACHES = 100;
   private static final int NUM_THREADS = 200;
   private static final int NUM_SEGMENTS = 1000;
   private static final int TIMEOUT_SECONDS = 180;

   public static final int JGROUPS_MAX_THREADS = 50;
   public static final int TRANSPORT_MAX_THREADS = 10;
   public static final int TRANSPORT_QUEUE_SIZE = 1000;
   public static final int REMOTE_MAX_THREADS = 50;
   public static final int REMOTE_QUEUE_SIZE = 0;
   public static final int STATE_TRANSFER_MAX_THREADS = 10;
   public static final int STATE_TRANSFER_QUEUE_SIZE = 0;

   @Override
   protected void createCacheManagers() throws Throwable {
      // start the cache managers in the test itself
   }

   public void testLargeClusterStart() throws Exception {
      if ((NUM_CACHES & 1) != 0)
         throw new IllegalStateException("NUM_CACHES must be even");

      final ProtocolStackConfigurator configurator = ConfiguratorFactory.getStackConfigurator("default-configs/default-jgroups-udp.xml");
      ProtocolConfiguration udpConfiguration = configurator.getProtocolStack().get(0);
      assertEquals("UDP", udpConfiguration.getProtocolName());
      udpConfiguration.getProperties().put("mcast_addr", "224.0.0.15");
      udpConfiguration.getProperties().put("thread_pool.min_threads", "0");
      udpConfiguration.getProperties().put("thread_pool.max_threads", String.valueOf(JGROUPS_MAX_THREADS));
      ProtocolConfiguration gmsConfiguration = configurator.getProtocolStack().get(9);
      assertEquals("pbcast.GMS", gmsConfiguration.getProtocolName());
      gmsConfiguration.getProperties().put("join_timeout", "5000");

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
      final CountDownLatch managersLatch = new CountDownLatch(NUM_NODES);
      ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS, getTestThreadFactory("Worker"));
      final ExecutorCompletionService<Void> managerCompletionService = new ExecutorCompletionService<>(executor);
//      final ExecutorCompletionService<Void> cacheCompletionService = new ExecutorCompletionService<Void>(new WithinThreadExecutor());
      final ExecutorCompletionService<Void> cacheCompletionService = new ExecutorCompletionService<Void>(executor);
      try {
         for (int nodeIndex = 0; nodeIndex < NUM_NODES; nodeIndex++) {
            final String nodeName = TestResourceTracker.getNameForIndex(nodeIndex);
            final String machineId = "m" + (nodeIndex / 2);
            managerCompletionService.submit(new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
                  gcb.globalJmxStatistics().allowDuplicateDomains(true);
                  gcb.transport().defaultTransport().nodeName(nodeName)
                        .addProperty(JGroupsTransport.CONFIGURATION_STRING, configurator.getProtocolStackString());
                  BlockingThreadPoolExecutorFactory transportExecutorFactory = new BlockingThreadPoolExecutorFactory(
                        TRANSPORT_MAX_THREADS, TRANSPORT_MAX_THREADS, TRANSPORT_QUEUE_SIZE, 60000);
                  gcb.transport().transportThreadPool().threadPoolFactory(transportExecutorFactory);
                  BlockingThreadPoolExecutorFactory remoteExecutorFactory = new BlockingThreadPoolExecutorFactory(
                        REMOTE_MAX_THREADS, REMOTE_MAX_THREADS, REMOTE_QUEUE_SIZE, 60000);
                  gcb.transport().remoteCommandThreadPool().threadPoolFactory(remoteExecutorFactory);
                  BlockingThreadPoolExecutorFactory stateTransferExecutorFactory = new
                        BlockingThreadPoolExecutorFactory(
                        STATE_TRANSFER_MAX_THREADS, STATE_TRANSFER_MAX_THREADS, STATE_TRANSFER_QUEUE_SIZE, 60000);
                  gcb.transport().stateTransferThreadPool().threadPoolFactory(stateTransferExecutorFactory);
                  final EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build());
                  try {
                     for (int i = 0; i < NUM_CACHES / 2; i++) {
                        final int cacheIndex = i;
                        cm.defineConfiguration("repl-cache-" + cacheIndex, replConfig);
                        cm.defineConfiguration("dist-cache-" + cacheIndex, distConfig);
                        cacheCompletionService.submit(new Callable<Void>() {
                           @Override
                           public Void call() throws Exception {
                              String cacheName = "repl-cache-" + cacheIndex;
                              Thread.currentThread().setName(cacheName + "-start-thread," + nodeName);
                              Cache<Object, Object> replCache = cm.getCache(cacheName);
//                              replCache.put(cm.getAddress(), "bla");
                              return null;
                           }
                        });
                        cacheCompletionService.submit(new Callable<Void>() {
                           @Override
                           public Void call() throws Exception {
                              String cacheName = "dist-cache-" + cacheIndex;
                              Thread.currentThread().setName(cacheName + "-start-thread," + nodeName);
                              Cache<Object, Object> distCache = cm.getCache(cacheName);
//                              distCache.put(cm.getAddress(), "bla");
                              return null;
                           }
                        });
                        managersLatch.countDown();
                     }
                  } finally {
                     registerCacheManager(cm);
                  }
                  log.infof("Started cache manager %s", nodeName);
                  return null;
               }
            });
         }

         long endTime = System.nanoTime() + SECONDS.toNanos(TIMEOUT_SECONDS);

         for (int i = 0; i < NUM_NODES; i++) {
            Future<Void> future = managerCompletionService.poll(TIMEOUT_SECONDS, SECONDS);
            future.get(0, SECONDS);
            if (System.nanoTime() - endTime > 0) {
               throw new TimeoutException("Took too long to start the cluster");
            }
         }

         int i = 0;
         while (i < NUM_NODES * NUM_CACHES) {
            Future<Void> future = cacheCompletionService.poll(1, SECONDS);
            if (future != null) {
               future.get(0, SECONDS);
               i++;
            }
            if (System.nanoTime() - endTime > 0) {
               throw new TimeoutException("Took too long to start the cluster");
            }
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
