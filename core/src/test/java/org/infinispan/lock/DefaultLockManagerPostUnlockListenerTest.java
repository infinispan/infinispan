package org.infinispan.lock;

import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;
import static org.infinispan.test.TestingUtil.named;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.concurrent.locks.impl.DefaultLockManager;
import org.infinispan.util.concurrent.locks.impl.PerKeyLockContainer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "lock.DefaultLockManagerPostUnlockListenerTest")
public class DefaultLockManagerPostUnlockListenerTest extends AbstractInfinispanTest {

   private DefaultLockManager lockManager;
   private final Object lockOwner = "owner";

   @BeforeMethod
   public void setUp() {
      lockManager = new DefaultLockManager();
      PerKeyLockContainer lockContainer = new PerKeyLockContainer();
      TestingUtil.inject(lockContainer, AbstractCacheTest.TIME_SERVICE);

      WithinThreadExecutor executor = new WithinThreadExecutor();
      ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
      ScheduledFuture future = mock(ScheduledFuture.class);
      when(future.cancel(anyBoolean())).thenReturn(true);
      //noinspection unchecked
      when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(future);
      //noinspection unchecked
      when(scheduler.schedule(any(Callable.class), anyLong(), any(TimeUnit.class))).thenReturn(future);

      TestingUtil.inject(lockManager, lockContainer, named(NON_BLOCKING_EXECUTOR, executor),
            named(TIMEOUT_SCHEDULE_EXECUTOR, scheduler));
   }

   public void testAddListenerThrowsWhenKeyNotLocked() {
      Supplier<CompletionStage<Void>> listener = () -> CompletableFuture.completedFuture(null);
      try {
         lockManager.addPostUnlockListener("k", listener);
         fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
         // expected
      }
   }

   public void testAddListenerSucceedsWhenKeyLocked() throws InterruptedException {
      lockManager.lock("k", lockOwner, 1, TimeUnit.MINUTES).lock();
      lockManager.addPostUnlockListener("k", () -> CompletableFuture.completedFuture(null));
      lockManager.unlock("k", lockOwner);
   }

   public void testUnlockReturnsListeners() throws InterruptedException {
      lockManager.lock("k", lockOwner, 1, TimeUnit.MINUTES).lock();

      CompletableFuture<Void> cf = new CompletableFuture<>();
      Supplier<CompletionStage<Void>> listener = () -> cf;
      lockManager.addPostUnlockListener("k", listener);

      Collection<Supplier<CompletionStage<Void>>> returned = lockManager.unlock("k", lockOwner);
      assertEquals(1, returned.size());

      CompletionStage<Void> stage = returned.iterator().next().get();
      assertNotNull(stage);
      assertEquals(cf, stage);
   }

   public void testUnlockAllReturnsListeners() throws InterruptedException {
      lockManager.lock("k", lockOwner, 1, TimeUnit.MINUTES).lock();

      CompletableFuture<Void> cf = new CompletableFuture<>();
      Supplier<CompletionStage<Void>> listener = () -> cf;
      lockManager.addPostUnlockListener("k", listener);

      Collection<Supplier<CompletionStage<Void>>> returned = lockManager.unlockAll(java.util.List.of("k"), lockOwner);
      assertEquals(1, returned.size());

      CompletionStage<Void> stage = returned.iterator().next().get();
      assertNotNull(stage);
      assertEquals(cf, stage);
   }

   public void testMultipleListenersSameKey() throws InterruptedException {
      lockManager.lock("k", lockOwner, 1, TimeUnit.MINUTES).lock();

      Supplier<CompletionStage<Void>> listener1 = () -> CompletableFuture.completedFuture(null);
      Supplier<CompletionStage<Void>> listener2 = () -> CompletableFuture.completedFuture(null);
      lockManager.addPostUnlockListener("k", listener1);
      lockManager.addPostUnlockListener("k", listener2);

      Collection<Supplier<CompletionStage<Void>>> returned = lockManager.unlockAll(java.util.List.of("k"), lockOwner);
      assertEquals(2, returned.size());
   }

   public void testMultipleListenersReturnedInOrder() throws InterruptedException {
      lockManager.lock("k", lockOwner, 1, TimeUnit.MINUTES).lock();

      Supplier<CompletionStage<Void>> listener1 = () -> CompletableFuture.completedFuture(null);
      Supplier<CompletionStage<Void>> listener2 = () -> CompletableFuture.completedFuture(null);
      Supplier<CompletionStage<Void>> listener3 = () -> CompletableFuture.completedFuture(null);
      lockManager.addPostUnlockListener("k", listener1);
      lockManager.addPostUnlockListener("k", listener2);
      lockManager.addPostUnlockListener("k", listener3);

      var returned = new java.util.ArrayList<>(lockManager.unlockAll(java.util.List.of("k"), lockOwner));
      assertEquals(3, returned.size());
      assertSame(listener1, returned.get(0));
      assertSame(listener2, returned.get(1));
      assertSame(listener3, returned.get(2));
   }

   public void testListenersAreOneShot() throws InterruptedException {
      lockManager.lock("k", lockOwner, 1, TimeUnit.MINUTES).lock();
      lockManager.addPostUnlockListener("k", () -> CompletableFuture.completedFuture(null));
      Collection<Supplier<CompletionStage<Void>>> first = lockManager.unlockAll(java.util.List.of("k"), lockOwner);
      assertEquals(1, first.size());

      lockManager.lock("k", lockOwner, 1, TimeUnit.MINUTES).lock();
      Collection<Supplier<CompletionStage<Void>>> second = lockManager.unlockAll(java.util.List.of("k"), lockOwner);
      assertTrue(second.isEmpty());
   }

   public void testMultipleKeysIndependent() throws InterruptedException {
      lockManager.lock("k1", lockOwner, 1, TimeUnit.MINUTES).lock();
      lockManager.lock("k2", lockOwner, 1, TimeUnit.MINUTES).lock();

      CompletableFuture<Void> cf1 = new CompletableFuture<>();
      CompletableFuture<Void> cf2 = new CompletableFuture<>();
      lockManager.addPostUnlockListener("k1", () -> cf1);
      lockManager.addPostUnlockListener("k2", () -> cf2);

      Collection<Supplier<CompletionStage<Void>>> returned =
            lockManager.unlockAll(java.util.List.of("k1", "k2"), lockOwner);
      assertEquals(2, returned.size());
   }

   public void testDifferentOwnersKeepListenersIndependent() throws InterruptedException {
      String owner1 = "owner1";
      String owner2 = "owner2";
      lockManager.lock("k1", owner1, 1, TimeUnit.MINUTES).lock();
      lockManager.lock("k2", owner2, 1, TimeUnit.MINUTES).lock();

      Supplier<CompletionStage<Void>> listener1 = () -> CompletableFuture.completedFuture(null);
      Supplier<CompletionStage<Void>> listener2 = () -> CompletableFuture.completedFuture(null);
      lockManager.addPostUnlockListener("k1", listener1);
      lockManager.addPostUnlockListener("k2", listener2);

      // Unlocking owner1 should only return owner1's listener
      Collection<Supplier<CompletionStage<Void>>> returned1 = lockManager.unlock("k1", owner1);
      assertEquals(1, returned1.size());
      assertSame(listener1, returned1.iterator().next());

      // Unlocking owner2 should only return owner2's listener
      Collection<Supplier<CompletionStage<Void>>> returned2 = lockManager.unlock("k2", owner2);
      assertEquals(1, returned2.size());
      assertSame(listener2, returned2.iterator().next());
   }
}
