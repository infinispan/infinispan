package org.infinispan.xsite.irac;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.infinispan.commands.RequestUUID;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link IracQueueCleanup}: a key is removed only if it is old enough, this node is the primary owner,
 * and it has neither an entry nor a tombstone in both checks, without a write in between.
 *
 * @since 16.4
 */
@Test(groups = "unit", testName = "xsite.irac.IracQueueCleanupUnitTest")
public class IracQueueCleanupUnitTest extends AbstractInfinispanTest {

   private static final long MIN_AGE_MILLIS = TimeUnit.HOURS.toMillis(1);
   private static final IracQueueCleanup.Settings SETTINGS = new IracQueueCleanup.Settings(MIN_AGE_MILLIS, 50, 2, 1);

   private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(getTestThreadFactory("cleanup"));
   private final AtomicLong requestId = new AtomicLong();
   private ControlledTimeService timeService;
   private FakeQueue queue;
   private IracQueueCleanup cleanup;

   @BeforeMethod(alwaysRun = true)
   public void setUp() {
      timeService = new ControlledTimeService();
      queue = new FakeQueue();
      cleanup = new IracQueueCleanup(queue, timeService, executor);
   }

   @AfterClass(alwaysRun = true)
   public void shutdown() {
      executor.shutdownNow();
   }

   public void testRemovesOldKeyMissingInBothChecks() {
      IracManagerKeyState state = queue.track("orphan");
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertEquals(List.of(state), queue.removed);
      assertFalse(queue.contains("orphan"));
      assertEquals(new IracQueueCleanupStatus("COMPLETED", 0, 0, 1, 1, 1), status);
   }

   public void testKeepsKeyYoungerThanMinAge() {
      queue.track("young");
      timeService.advance(MIN_AGE_MILLIS - 1, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertTrue(queue.removed.isEmpty());
      assertEquals(0, status.checked(), "A key younger than the minimum age must not be loaded");
      assertEquals(MIN_AGE_MILLIS - 1, status.oldestKeyAgeMillis());
   }

   public void testKeepsKeyWithEntryOrTombstone() {
      queue.track("present");
      queue.missing = state -> CompletableFuture.completedFuture(false);
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertTrue(queue.removed.isEmpty());
      assertEquals(1, status.checked());
      assertEquals(0, status.candidates());
   }

   public void testKeepsKeyNotPrimary() {
      queue.track("backup-owner", 1);
      queue.notPrimarySegments.add(1);
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertTrue(queue.removed.isEmpty());
      assertEquals(0, status.checked());
   }

   public void testKeepsKeyWhenLoadFails() {
      queue.track("load-failure");
      queue.missing = state -> CompletableFuture.failedFuture(new RuntimeException("injected"));
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertTrue(queue.removed.isEmpty());
      assertEquals(0, status.candidates());
   }

   /**
    * The entry is found in the second check, for example, after the passivation finished.
    */
   public void testKeepsKeyFoundInSecondCheck() {
      queue.track("passivated");
      AtomicLong loads = new AtomicLong();
      queue.missing = state -> CompletableFuture.completedFuture(loads.incrementAndGet() == 1);
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertTrue(queue.removed.isEmpty());
      assertEquals(1, status.candidates());
      assertEquals(0, status.removed());
      assertEquals(2, loads.get());
   }

   /**
    * A write between both checks replaces the queued state; the new update must be sent, so the key is kept.
    */
   public void testKeepsKeyUpdatedBetweenChecks() {
      queue.track("updated");
      queue.missing = state -> {
         // the write happens right after the first check
         if (queue.current("updated") == state) {
            queue.track("updated");
         }
         return CompletableFuture.completedFuture(true);
      };
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertTrue(queue.removed.isEmpty());
      assertTrue(queue.contains("updated"));
      assertEquals(1, status.candidates());
      assertEquals(0, status.removed());
   }

   /**
    * The primary owner changes between both checks; the new primary owner is responsible for the key.
    */
   public void testKeepsKeyWhenPrimaryChangesBetweenChecks() {
      queue.track("moved", 1);
      queue.missing = state -> {
         queue.notPrimarySegments.add(1);
         return CompletableFuture.completedFuture(true);
      };
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertTrue(queue.removed.isEmpty());
      assertEquals(1, status.candidates());
      assertEquals(0, status.removed());
   }

   public void testFailedRemovalIsReported() {
      queue.track("orphan");
      queue.failRemove = true;
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertEquals("FAILED", status.state());
      assertEquals(0, status.removed());
   }

   public void testOnlyMissingKeysAreRemovedInBatches() {
      List<IracManagerKeyState> orphans = new ArrayList<>();
      for (int i = 0; i < 5; ++i) {
         orphans.add(queue.track("orphan-" + i));
         queue.track("present-" + i);
      }
      queue.missing = state -> CompletableFuture.completedFuture(((String) state.getKey()).startsWith("orphan"));
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);

      IracQueueCleanupStatus status = runToCompletion();

      assertEquals(Set.copyOf(orphans), Set.copyOf(queue.removed));
      assertEquals(new IracQueueCleanupStatus("COMPLETED", 5, MIN_AGE_MILLIS, 10, 5, 5), status);
   }

   public void testCancel() {
      queue.track("orphan");
      timeService.advance(MIN_AGE_MILLIS, TimeUnit.MILLISECONDS);
      IracQueueCleanup.Settings slow = new IracQueueCleanup.Settings(MIN_AGE_MILLIS, TimeUnit.MINUTES.toMillis(10), 2, 1);

      assertEquals("RUNNING", cleanup.start(slow).state());
      // a second start while running is a no-op
      assertEquals("RUNNING", cleanup.start(slow).state());
      eventuallyEquals(1L, () -> cleanup.status().candidates());

      assertEquals("CANCELLED", cleanup.cancel().state());
      assertTrue(queue.removed.isEmpty());
      assertTrue(queue.contains("orphan"));
   }

   private IracQueueCleanupStatus runToCompletion() {
      assertEquals("RUNNING", cleanup.start(SETTINGS).state());
      eventually(() -> !"RUNNING".equals(cleanup.status().state()));
      return cleanup.status();
   }

   private class FakeQueue implements IracQueueCleanup.Queue {
      private final Map<Object, IracManagerKeyState> states = new ConcurrentHashMap<>();
      private final Set<Integer> notPrimarySegments = ConcurrentHashMap.newKeySet();
      private final List<IracManagerKeyState> removed = new ArrayList<>();
      private volatile boolean failRemove;
      private volatile Function<IracManagerKeyState, CompletionStage<Boolean>> missing =
            state -> CompletableFuture.completedFuture(true);

      IracManagerKeyState track(String key) {
         return track(key, 0);
      }

      IracManagerKeyState track(String key, int segment) {
         IracManagerKeyState state = new IracManagerKeyChangedState(
               new IracManagerKeyInfo(segment, key, RequestUUID.localOf(requestId.incrementAndGet())), false, 1,
               timeService.time());
         states.put(key, state);
         return state;
      }

      boolean contains(Object key) {
         return states.containsKey(key);
      }

      @Override
      public String cacheName() {
         return "test-cache";
      }

      @Override
      public Collection<IracManagerKeyState> snapshot() {
         return List.copyOf(states.values());
      }

      @Override
      public IracManagerKeyState current(Object key) {
         return states.get(key);
      }

      @Override
      public boolean isPrimary(int segment) {
         return !notPrimarySegments.contains(segment);
      }

      @Override
      public CompletionStage<Boolean> isMissing(IracManagerKeyState state) {
         return missing.apply(state);
      }

      @Override
      public synchronized void remove(Collection<IracManagerKeyState> toRemove) {
         if (failRemove) {
            throw new IllegalStateException("injected");
         }
         for (IracManagerKeyState state : toRemove) {
            state.discard();
            states.remove(state.getKey(), state);
            removed.add(state);
         }
      }
   }
}
