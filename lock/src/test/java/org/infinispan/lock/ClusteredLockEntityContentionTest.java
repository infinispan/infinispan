package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.lock.impl.ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME;
import static org.infinispan.test.TestingUtil.getListeners;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Reproduces the reported usage of clustered locks: many short lived locks, one per entity id, taken concurrently by
 * several nodes that receive data for the same entity from different sources.
 * <p>
 * The lock must remain strictly exclusive across the cluster and the number of locks must not change the number of
 * cluster listeners installed on the lock cache.
 */
@Test(groups = "functional", testName = "clusteredLock.ClusteredLockEntityContentionTest")
public class ClusteredLockEntityContentionTest extends BaseClusteredLockTest {

   private static final int ENTITIES = 20;
   private static final int SHORT_LIVED_LOCKS = 50;
   private static final String LOCK_PREFIX = "entity-";

   @Override
   public Object[] factory() {
      return new Object[]{
            // REPL
            new ClusteredLockEntityContentionTest().numOwner(-1),
            // DIST
            new ClusteredLockEntityContentionTest().numOwner(2),
      };
   }

   @AfterMethod(alwaysRun = true)
   public void removeLocks() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      for (int entity = 0; entity < ENTITIES; entity++) {
         if (clusteredLockManager.isDefined(entityLock(entity))) {
            await(clusteredLockManager.remove(entityLock(entity)));
         }
      }
   }

   public void testEntityLocksStayExclusiveUnderContention() throws Exception {
      ClusteredLockManager definingManager = clusteredLockManager(0);
      for (int entity = 0; entity < ENTITIES; entity++) {
         definingManager.defineLock(entityLock(entity));
      }

      AtomicInteger[] holders = newCounters(ENTITIES);
      AtomicInteger[] updates = newCounters(ENTITIES);
      AtomicInteger exclusionViolations = new AtomicInteger();

      List<Future<Void>> sources = new ArrayList<>(clusterSize());
      for (int node = 0; node < clusterSize(); node++) {
         ClusteredLockManager source = clusteredLockManager(node);
         sources.add(fork(() -> {
            for (int entity = 0; entity < ENTITIES; entity++) {
               ClusteredLock lock = source.get(entityLock(entity));
               await(lock.lock());
               try {
                  if (holders[entity].incrementAndGet() != 1) {
                     exclusionViolations.incrementAndGet();
                  }
                  // Widen the critical section so that a lost exclusion would actually overlap
                  TestingUtil.sleepThread(1);
                  updates[entity].incrementAndGet();
                  if (holders[entity].decrementAndGet() != 0) {
                     exclusionViolations.incrementAndGet();
                  }
               } finally {
                  await(lock.unlock());
               }
            }
            return null;
         }));
      }

      for (Future<Void> source : sources) {
         source.get(120, TimeUnit.SECONDS);
      }

      assertEquals(0, exclusionViolations.get(), "The lock was held by more than one node at the same time");
      for (int entity = 0; entity < ENTITIES; entity++) {
         assertEquals(clusterSize(), updates[entity].get(), "Lost update on " + entityLock(entity));
         assertFalse(await(clusteredLockManager(0).get(entityLock(entity)).isLocked()));
      }
   }

   public void testShortLivedLocksDoNotAddListeners() {
      Cache<Object, Object> lockCache = cache(0, CLUSTERED_LOCK_CACHE_NAME);
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);

      // Baseline taken after the first lock, which is what installs the single listener of this node
      clusteredLockManager.defineLock(entityLock(0));
      assertTrue(await(clusteredLockManager.get(entityLock(0)).tryLock()));
      await(clusteredLockManager.get(entityLock(0)).unlock());
      int listeners = getListeners(lockCache).size();

      for (int round = 0; round < SHORT_LIVED_LOCKS; round++) {
         String name = LOCK_PREFIX + "short-" + round;
         clusteredLockManager.defineLock(name);
         ClusteredLock lock = clusteredLockManager.get(name);
         assertTrue(await(lock.tryLock()));
         await(lock.unlock());
         assertTrue(await(clusteredLockManager.remove(name)));
      }

      assertEquals(listeners, getListeners(lockCache).size());
   }

   public void testLockIsUsableAfterItIsRemovedAndDefinedAgain() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      ClusteredLockManager otherManager = clusteredLockManager(1);

      for (int round = 0; round < 5; round++) {
         clusteredLockManager.defineLock(entityLock(0));

         ClusteredLock lock = clusteredLockManager.get(entityLock(0));
         assertTrue(await(lock.tryLock()));
         assertFalse(await(otherManager.get(entityLock(0)).tryLock()));
         await(lock.unlock());

         assertTrue(await(clusteredLockManager.remove(entityLock(0))));
      }
   }

   private static AtomicInteger[] newCounters(int size) {
      AtomicInteger[] counters = new AtomicInteger[size];
      for (int i = 0; i < size; i++) {
         counters[i] = new AtomicInteger();
      }
      return counters;
   }

   private static String entityLock(int entity) {
      return LOCK_PREFIX + entity;
   }
}
