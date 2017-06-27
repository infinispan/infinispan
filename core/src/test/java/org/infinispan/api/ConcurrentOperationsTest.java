package org.infinispan.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.ConcurrentOperationsTest")
public class ConcurrentOperationsTest extends MultipleCacheManagersTest {

   protected final int threads;
   protected final int nodes;
   protected final int operations;
   protected final CacheMode cacheMode;

   protected ConcurrentOperationsTest(CacheMode cacheMode, int threads, int nodes, int operations) {
      this.cacheMode = cacheMode;
      this.threads = threads;
      this.nodes = nodes;
      this.operations = operations;
   }

   public ConcurrentOperationsTest() {
      this(CacheMode.DIST_SYNC, 2, 2, 4);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(cacheMode, false);
      dcc.clustering().l1().disable();
      createClusteredCaches(nodes, dcc);
   }

   public void testNoTimeout() throws Throwable {
      runTest(false);
   }

   public void testNoTimeoutAndCorrectness() throws Throwable {
      runTest(true);
   }

   private void runTest(final boolean checkCorrectness) throws Throwable {
      final CyclicBarrier barrier = new CyclicBarrier(threads);
      final Random rnd = new Random();
      final AtomicBoolean correctness = new AtomicBoolean(Boolean.TRUE);
      List<Future<Boolean>> result = new ArrayList<Future<Boolean>>();
      for (int t = 0; t < threads; t++) {
         final int part = t;
         Future<Boolean> f = fork(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
               try {
                  for (int i = 0; i < operations; i++) {
                     barrier();
                     executeOperation(i);
                     barrier();
                     checkCorrectness(i);
                     printProgress(i);
                     if (!correctness.get()) break;

                  }
               } catch (Throwable t) {
                  correctness.set(false);
                  throw new Exception(t);
               }
               return correctness.get();
            }

            private void printProgress(int i) {
               if (i % 100 == 0) print("Progressing  = " + i);
            }

            private void executeOperation(int iteration) {
               int node = rnd.nextInt(nodes - 1);
               switch (rnd.nextInt(4)) {
                  case 0: {
                     cache(node).put("k", "v_" + part + "_" + iteration);
                     break;
                  }
                  case 1: {
                     cache(node).remove("k");
                     break;
                  }
                  case 2: {
                     cache(node).putIfAbsent("k", "v" + part);
                     break;
                  }
                  case 3: {
                     cache(node).replace("k", "v" + part);
                     break;
                  }
                  default:
                     throw new IllegalStateException();
               }
            }

            private void checkCorrectness(int i) {
               if (checkCorrectness) {
                  log.tracef("Checking correctness for iteration %s", i);
                  print("Checking correctness");

                  List<Address> owners = advancedCache(0).getDistributionManager().locate("k");
                  if (!checkOwners(owners)) {
                     correctness.set(false);
                  }

                  for (int q = 0; q < nodes; q++) {
                     print(q, cache(0).get("k"));
                  }

                  Object expectedValue = cache(0).get("k");
                  log.tracef("Original value read from cache 0 is %s", expectedValue);
                  for (int j = 0; j < nodes; j++) {
                     Object actualValue = cache(j).get("k");
                     boolean areEquals = expectedValue == null ? actualValue == null : expectedValue.equals(actualValue);
                     print("Are " + actualValue + " and " + expectedValue + " equals ? " + areEquals);
                     if (!areEquals) {
                        correctness.set(false);
                        print("Consistency error. On cache 0 we had " + expectedValue + " and on " + j + " we had " + actualValue);
                        log.trace("Consistency error. On cache 0 we had " + expectedValue + " and on " + j + " we had " + actualValue);
                     }

                  }
               }
            }

            private void barrier() throws BrokenBarrierException, java.util.concurrent.TimeoutException, InterruptedException {
               barrier.await(10000, TimeUnit.MILLISECONDS);
               log.tracef("Just passed barrier.");
            }

         });
         result.add(f);
      }

      for (Future<Boolean> f: result) {
         assertTrue(f.get());
      }
   }

   protected boolean checkOwners(List<Address> owners) {
      assert owners.size() == 2;

      InternalCacheEntry entry0 = advancedCache(owners.get(0)).getDataContainer().get("k");
      InternalCacheEntry entry1 = advancedCache(owners.get(1)).getDataContainer().get("k");
      return checkOwnerEntries(entry0, entry1, owners.get(0), owners.get(1));
   }

   protected boolean checkOwnerEntries(InternalCacheEntry entry0, InternalCacheEntry entry1, Address mainOwner, Address backupOwner) {
      Object mainOwnerValue = entry0 == null ? null : entry0.getValue();
      Object otherOwnerValue = entry1 == null ? null : entry1.getValue();
      log.tracef("Main owner value is %s, other Owner Value is %s", mainOwnerValue, otherOwnerValue);
      boolean equals = mainOwnerValue == null? otherOwnerValue == null : mainOwnerValue.equals(otherOwnerValue);
      if (!equals) {
         print("Consistency error. On main owner(" + mainOwner + ") we had " +
            mainOwnerValue + " and on backup owner(" + backupOwner + ") we had " + otherOwnerValue);
         log.trace("Consistency error. On main owner(" + mainOwner + ") we had " +
            mainOwnerValue + " and on backup owner(" + backupOwner + ") we had " + otherOwnerValue);
         return false;
      }

      print("otherOwnerValue = " + otherOwnerValue);
      print("mainOwnerValue = " + mainOwnerValue);
      return true;
   }

   private AdvancedCache advancedCache(Address address) {
      for (Cache c : caches()) {
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(address))
            return c.getAdvancedCache();
      }
      throw new IllegalStateException("Couldn't find cache for address : " + address);
   }

   private void print(int index, Object value) {
      print("[" + Thread.currentThread().getName() + "] Cache " + index + " sees value " + value);
   }

   private void print(Object value) {
      log.debug(value);
   }

   public void testReplace() {
      cache(0).put("k", "v1");
      for (int i = 0; i < nodes; i++) {
         assertEquals("v1", cache(i).get("k"));
      }
      assert cache(0).replace("k", "v2") != null;
      assert cache(0).replace("k", "v2", "v3");
      assertEquals(cache(0).get("k"), "v3");
   }
}
