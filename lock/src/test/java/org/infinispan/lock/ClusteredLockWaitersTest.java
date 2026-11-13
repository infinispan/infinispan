package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.lock.impl.ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME;
import static org.infinispan.testing.Exceptions.assertException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.util.ByteString;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Checks the waiter bookkeeping that replaces the per lock cluster listener: a node that fails to acquire a lock
 * registers itself in the lock value and is notified with a unicast when the lock is released or removed.
 */
@Test(groups = "functional", testName = "clusteredLock.ClusteredLockWaitersTest")
public class ClusteredLockWaitersTest extends BaseClusteredLockTest {

   private static final String LOCK_NAME = "ClusteredLockWaitersTest";

   @Override
   public Object[] factory() {
      return new Object[]{
            // REPL
            new ClusteredLockWaitersTest().numOwner(-1),
            // DIST, the lock key is not owned by every node
            new ClusteredLockWaitersTest().numOwner(1),
            new ClusteredLockWaitersTest().numOwner(2),
      };
   }

   @BeforeMethod(alwaysRun = true)
   public void createLock() {
      clusteredLockManager(0).defineLock(LOCK_NAME, new ClusteredLockConfiguration());
   }

   @AfterMethod(alwaysRun = true)
   public void destroyLock() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      if (clusteredLockManager.isDefined(LOCK_NAME)) {
         await(clusteredLockManager.forceRelease(LOCK_NAME));
         await(clusteredLockManager.remove(LOCK_NAME));
      }
   }

   public void testFailedAcquisitionRegistersTheNodeAsWaiter() {
      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);
      ClusteredLock lock2 = clusteredLockManager(2).get(LOCK_NAME);

      await(lock0.lock());
      assertEquals(Set.of(), lockValue().getWaiters());

      CompletableFuture<Void> lock1Request = lock1.lock();
      eventually(() -> lockValue().hasWaiter(waiterId(1)));

      CompletableFuture<Boolean> lock2Request = lock2.tryLock(30, TimeUnit.SECONDS);
      eventually(() -> lockValue().hasWaiter(waiterId(2)));
      assertEquals(Set.of(waiterId(1), waiterId(2)), lockValue().getWaiters());

      await(lock0.unlock());

      // Both waiters are notified: one of them acquires the lock and the other one is served when it is released
      await(CompletableFuture.allOf(
            lock1Request.thenCompose(ignored -> lock1.unlock()),
            lock2Request.thenCompose(acquired -> {
               assertTrue(acquired);
               return lock2.unlock();
            })));
      assertFalse(await(lock0.isLocked()));
   }

   public void testWaitersAreDroppedOnANewOwnershipEpoch() {
      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);

      await(lock0.lock());
      CompletableFuture<Boolean> lock1Request = lock1.tryLock(50, TimeUnit.MILLISECONDS);
      eventually(() -> lockValue().hasWaiter(waiterId(1)));

      // The request expires but the stale waiter is not cleaned up explicitly
      assertFalse(await(lock1Request));
      assertTrue(lockValue().hasWaiter(waiterId(1)));

      // The released value still carries the waiters so that they can be notified
      await(lock0.unlock());
      assertEquals(ClusteredLockState.RELEASED, lockValue().getState());
      assertTrue(lockValue().hasWaiter(waiterId(1)));

      // The next acquisition starts a new epoch and discards the stale waiter
      await(lock0.lock());
      assertEquals(Set.of(), lockValue().getWaiters());
      await(lock0.unlock());
   }

   public void testTryLockWithoutTimeoutDoesNotRegisterAWaiter() {
      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);

      assertTrue(await(lock0.tryLock()));
      assertFalse(await(lock1.tryLock()));
      assertFalse(await(lock1.tryLock(0, TimeUnit.SECONDS)));
      assertFalse(await(lock1.tryLock(-1, TimeUnit.SECONDS)));

      // A request that does not wait for the lock does not need to be notified
      assertEquals(Set.of(), lockValue().getWaiters());
      await(lock0.unlock());
   }

   public void testConcurrentRequestsFromTheSameNodeAreAllServed() {
      ClusteredLock lock = clusteredLockManager(0).get(LOCK_NAME);

      assertTrue(await(lock.tryLock()));
      CompletableFuture<Boolean> first = lock.tryLock(30, TimeUnit.SECONDS);
      CompletableFuture<Boolean> second = lock.tryLock(30, TimeUnit.SECONDS);
      eventually(() -> lockValue().hasWaiter(waiterId(0)));

      await(lock.unlock());
      assertTrue(await(first));

      // The node must stay registered while it still has a pending request, otherwise the second request hangs
      assertTrue(lockValue().hasWaiter(waiterId(0)));
      await(lock.unlock());
      assertTrue(await(second));
      await(lock.unlock());
   }

   public void testForceReleaseNotifiesTheWaiters() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      ClusteredLock lock0 = clusteredLockManager.get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);

      await(lock0.lock());
      CompletableFuture<Void> lock1Request = lock1.lock();
      eventually(() -> lockValue().hasWaiter(waiterId(1)));

      assertTrue(await(clusteredLockManager.forceRelease(LOCK_NAME)));

      await(lock1Request);
      assertTrue(await(lock1.isLockedByMe()));
      await(lock1.unlock());
   }

   public void testRemoveNotifiesTheWaiters() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      ClusteredLock lock0 = clusteredLockManager.get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);
      ClusteredLock lock2 = clusteredLockManager(2).get(LOCK_NAME);

      await(lock0.lock());
      CompletableFuture<Void> lock1Request = lock1.lock();
      CompletableFuture<Boolean> lock2Request = lock2.tryLock(30, TimeUnit.SECONDS);
      eventually(() -> lockValue().hasWaiter(waiterId(1)) && lockValue().hasWaiter(waiterId(2)));

      assertTrue(await(clusteredLockManager.remove(LOCK_NAME)));

      assertLockDeleted(lock1Request);
      assertLockDeleted(lock2Request);
   }

   public void testLockIsHandedOverAcrossNodes() {
      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);
      ClusteredLock lock2 = clusteredLockManager(2).get(LOCK_NAME);

      await(lock0.lock());
      CompletableFuture<Void> lock1Request = lock1.lock();
      CompletableFuture<Void> lock2Request = lock2.lock();
      eventually(() -> lockValue().hasWaiter(waiterId(1)) && lockValue().hasWaiter(waiterId(2)));

      await(lock0.unlock());
      // Whoever wins the race releases the lock, so the other one is notified in turn
      await(CompletableFuture.allOf(
            lock1Request.thenCompose(ignored -> lock1.unlock()),
            lock2Request.thenCompose(ignored -> lock2.unlock())));

      assertFalse(await(lock0.isLocked()));
   }

   public void testUncontendedRequestsDoNotPileUp() {
      ClusteredLock lock = clusteredLockManager(0).get(LOCK_NAME);

      for (int i = 0; i < 100; i++) {
         await(lock.lock());
         await(lock.unlock());
      }
      assertEquals(Set.of(), lockValue().getWaiters());
      assertEquals(ClusteredLockState.RELEASED, lockValue().getState());

      // A node with no pending request left must not be registered, so a last acquisition is uncontended
      assertTrue(await(lock.tryLock()));
      await(lock.unlock());
   }

   public void testAllContendingRequestsAreEventuallyServed() throws Exception {
      int rounds = 10;
      AtomicInteger counter = new AtomicInteger();
      List<Future<Void>> tasks = new ArrayList<>(clusterSize());

      for (int i = 0; i < clusterSize(); i++) {
         ClusteredLock lock = clusteredLockManager(i).get(LOCK_NAME);
         tasks.add(fork(() -> {
            for (int round = 0; round < rounds; round++) {
               await(lock.lock());
               counter.incrementAndGet();
               await(lock.unlock());
            }
            return null;
         }));
      }

      for (Future<Void> task : tasks) {
         task.get(60, TimeUnit.SECONDS);
      }
      assertEquals(clusterSize() * rounds, counter.get());
      assertFalse(await(clusteredLockManager(0).get(LOCK_NAME).isLocked()));
   }

   public void testLockValueIsReplicatedWithItsWaiters() {
      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);

      await(lock0.lock());
      CompletableFuture<Void> lock1Request = lock1.lock();
      eventually(() -> lockValue().hasWaiter(waiterId(1)));

      // The value, including the waiters, must survive the marshalling to the other members
      for (int i = 0; i < clusterSize(); i++) {
         int node = i;
         eventually("Unexpected lock value in node " + node, () -> {
            ClusteredLockValue value = lockValue(node);
            return value != null && Set.of(waiterId(1)).equals(value.getWaiters())
                  && ClusteredLockState.ACQUIRED == value.getState();
         });
      }

      await(lock0.unlock());
      await(lock1Request);
      await(lock1.unlock());
   }

   private void assertLockDeleted(CompletableFuture<?> request) {
      assertNull(await(request.exceptionally(e -> {
         assertException(ClusteredLockException.class, e);
         return null;
      })));
      assertTrue(request.isCompletedExceptionally());
   }

   private int waiterId(int nodeIndex) {
      return ClusteredLockValue.waiterId(manager(nodeIndex).getAddress());
   }

   private ClusteredLockValue lockValue() {
      return lockValue(0);
   }

   private ClusteredLockValue lockValue(int nodeIndex) {
      Cache<ClusteredLockKey, ClusteredLockValue> cache = cache(nodeIndex, CLUSTERED_LOCK_CACHE_NAME);
      return cache.get(new ClusteredLockKey(ByteString.fromString(LOCK_NAME)));
   }
}
