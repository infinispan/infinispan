package org.infinispan.partitionhandling;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.transaction.xa.XAException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.LockingMode;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "partitionhandling.PartitionStressTest")
public class PartitionStressTest extends MultipleCacheManagersTest {

   public static final int NUM_NODES = 4;

   @Override
   public Object[] factory() {
      return new Object[] {
         new PartitionStressTest().cacheMode(CacheMode.DIST_SYNC).transactional(false),
         new PartitionStressTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC),
         new PartitionStressTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
      };
   }

   public PartitionStressTest() {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(cacheMode);
      builder.transaction().transactionMode(transactionMode()).lockingMode(lockingMode);
      builder.clustering().partitionHandling().enabled(true);
      for (int i = 0; i < NUM_NODES; i++) {
         addClusterEnabledCacheManager(builder, new TransportFlags().withFD(true).withMerge(true));
      }
      waitForClusterToForm();
   }

   public void testWriteDuringPartition() throws Exception {
      DISCARD[] discards = new DISCARD[NUM_NODES];
      for (int i = 0; i < NUM_NODES; i++) {
         discards[i] = TestingUtil.getDiscardForCache(cache(i));
      }

      final List<Future<Object>> futures = new ArrayList<>(NUM_NODES);
      final ConcurrentMap<String, Integer> insertedKeys = CollectionFactory.makeConcurrentMap();
      final AtomicBoolean stop = new AtomicBoolean(false);
      for (int i = 0; i < NUM_NODES; i++) {
         final int cacheIndex = i;
         Future<Object> future = fork(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
               Cache<String, Integer> cache = cache(cacheIndex);
               int count = 0;
               while (!stop.get()) {
                  String key = "key" + cacheIndex + "_" + count;
                  try {
                     cache.put(key, count);
                     insertedKeys.put(key, count);
                  } catch (AvailabilityException e) {
                     // expected, ignore
                  } catch (CacheException e) {
                     if (e.getCause() instanceof XAException && e.getCause().getCause() instanceof AvailabilityException) {
                        // expected, ignore
                     }
                  }
                  count++;
                  Thread.sleep(0);
               }
               return count;
            }
         });
         futures.add(future);
      }

      long startTime = TIME_SERVICE.time();
      int splitIndex = 0;
      while (splitIndex < NUM_NODES) {
         List<Address> partitionOne = new ArrayList<Address>(NUM_NODES);
         List<Address> partitionTwo = new ArrayList<Address>(NUM_NODES);
         List<EmbeddedCacheManager> partitionOneManagers = new ArrayList<>();
         List<EmbeddedCacheManager> partitionTwoManagers = new ArrayList<>();
         for (int i = 0; i < NUM_NODES; i++) {
            if ((i + splitIndex) % NUM_NODES < NUM_NODES / 2) {
               partitionTwo.add(address(i));
               partitionTwoManagers.add(manager(i));
            } else {
               partitionOne.add(address(i));
               partitionOneManagers.add(manager(i));
            }
         }
         assertEquals(NUM_NODES / 2, partitionTwo.size());
         log.infof("Cache is available, splitting cluster at index %d. First partition is %s, second partition is %s",
               splitIndex, partitionOne, partitionTwo);

         for (int i = 0; i < NUM_NODES; i++) {
            if (partitionOne.contains(address(i))) {
               for (Address a : partitionTwo) {
                  discards[i].addIgnoreMember(((JGroupsAddress) a).getJGroupsAddress());
               }
            } else {
               for (Address a : partitionOne) {
                  discards[i].addIgnoreMember(((JGroupsAddress) a).getJGroupsAddress());
               }
            }
         }
         TestingUtil.blockForMemberToFail(30000, partitionOneManagers.toArray(new CacheContainer[0]));
         TestingUtil.blockForMemberToFail(30000, partitionTwoManagers.toArray(new CacheContainer[0]));

         log.infof("Nodes split, waiting for the caches to become degraded");
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return TestingUtil.extractComponent(cache(0), PartitionHandlingManager.class).getAvailabilityMode() ==
                     AvailabilityMode.DEGRADED_MODE;
            }
         });

         assertFuturesRunning(futures);

         log.infof("Cache is degraded, merging partitions %s and %s", partitionOne, partitionTwo);
         for (int i = 0; i < NUM_NODES; i++) {
            discards[i].resetIgnoredMembers();
         }
         TestingUtil.blockUntilViewsReceived(60000, true, cacheManagers.toArray(new CacheContainer[0]));

         log.infof("Partitions merged, waiting for the caches to become available");
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return TestingUtil.extractComponent(cache(0), PartitionHandlingManager.class).getAvailabilityMode() ==
                     AvailabilityMode.AVAILABLE;
            }
         });
         TestingUtil.waitForStableTopology(caches());

         assertFuturesRunning(futures);
         splitIndex++;
      }

      stop.set(true);
      for (Future<Object> future : futures) {
         future.get(10, TimeUnit.SECONDS);
      }

      for (String key : insertedKeys.keySet()) {
         for (int i = 0; i < NUM_NODES; i++) {
            assertEquals("Failure for key " + key + " on " + cache(i), insertedKeys.get(key), cache(i).get(key));
         }
      }

      long duration = TIME_SERVICE.timeDuration(startTime, TimeUnit.SECONDS);
      log.infof("Test finished in %d seconds", duration);
   }

   protected void assertFuturesRunning(List<Future<Object>> futures) {
      for (Future<Object> future : futures) {
         assertFalse(future.isDone());
      }
   }
}
