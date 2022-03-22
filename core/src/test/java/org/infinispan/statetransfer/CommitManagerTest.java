package org.infinispan.statetransfer;

import org.infinispan.commons.time.TimeService;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "functional", testName = "statetransfer.CommitManagerTest")
public class CommitManagerTest {

   public void shouldStartAndStopTrackingCorrectly() {
      final CommitManager manager = new CommitManager();

      // Recently create manager is not tracking anything.
      assertFalse(manager.isTracking(Flag.PUT_FOR_STATE_TRANSFER));
      assertFalse(manager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));

      // Start tracking state transfer.
      manager.startTrack(Flag.PUT_FOR_STATE_TRANSFER);
      assertTrue(manager.isTracking(Flag.PUT_FOR_STATE_TRANSFER));

      // Stop tracking state transfer.
      manager.stopTrack(Flag.PUT_FOR_STATE_TRANSFER);
      assertFalse(manager.isTracking(Flag.PUT_FOR_STATE_TRANSFER));

      // Start tracking cross site state transfer.
      manager.startTrack(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
      assertTrue(manager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));

      // Stop tracking cross site state transfer.
      manager.stopTrack(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
      assertFalse(manager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));
   }

   public void shouldCommitEntriesForSegment() throws Exception {
      final int numberOfSegments = 10;

      final InternalDataContainer container = mock(InternalDataContainer.class);
      final PersistenceManager persistenceManager = mock(PersistenceManager.class);
      final TimeService timeService = mock(TimeService.class);

      final CommitManager manager = new CommitManager();
      TestingUtil.inject(manager, container, persistenceManager, timeService);

      // Start tracking for state transfer.
      manager.startTrack(Flag.PUT_FOR_STATE_TRANSFER);

      // Create some entries associated with segments.
      for (int i = 0; i < numberOfSegments; i++) {
         for (int j = 0; j < 10; j++) {
            String formatted = String.format("value-%d-%d", i, j);
            final CacheEntry<String, String> entry = new ReadCommittedEntry<>(formatted, formatted, null);
            manager.commit(entry, Flag.PUT_FOR_STATE_TRANSFER, i, false, null)
                  .toCompletableFuture().get(1, TimeUnit.SECONDS);
         }
      }

      // The map should not store any entries since we are tracking only state transfer and
      // the manager was fed with only state transfer entries.
      assertEquals(manager.tracker.size(), 0);
      assertTrue(manager.isEmpty());

      // Stop tracking some segments does not raise any problems.
      manager.stopTrackFor(Flag.PUT_FOR_STATE_TRANSFER, 0);
      manager.stopTrackFor(Flag.PUT_FOR_STATE_TRANSFER, 1);
      manager.stopTrackFor(Flag.PUT_FOR_STATE_TRANSFER, 2);

      // Verify that still tracking for state transfer and not entries were stored.
      assertTrue(manager.isTracking(Flag.PUT_FOR_STATE_TRANSFER));
      assertTrue(manager.isEmpty());
   }

   public void onlyClearSegmentIfNoXSiteST() throws Exception {
      final int numberOfSegments = 10;
      final IntPredicate isXSiteSegment = segment -> segment % 2 != 0;

      final InternalDataContainer container = mock(InternalDataContainer.class);
      final PersistenceManager persistenceManager = mock(PersistenceManager.class);
      final TimeService timeService = mock(TimeService.class);

      final CommitManager manager = new CommitManager();
      TestingUtil.inject(manager, container, persistenceManager, timeService);

      // Start tracking for state transfer.
      manager.startTrack(Flag.PUT_FOR_STATE_TRANSFER);
      manager.startTrack(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);

      // Create some entries associated with segments.
      for (int i = 0; i < numberOfSegments; i++) {
         for (int j = 0; j < 10; j++) {
            String formatted = String.format("value-%d-%d", i, j);
            final CacheEntry<String, String> entry = new ReadCommittedEntry<>(formatted, formatted, null);
            CompletionStage<?> future;
            if (isXSiteSegment.test(i)) {
               future = manager.commit(entry, Flag.PUT_FOR_X_SITE_STATE_TRANSFER, i, false, null);
            } else {
               future = manager.commit(entry, Flag.PUT_FOR_STATE_TRANSFER, i, false, null);
            }

            future.toCompletableFuture().get(1, TimeUnit.SECONDS);
         }
      }

      // Verify that we are tracking numberOfSegments segments. This is a different scenario because we are dealing
      // with both types simultaneously.
      assertEquals(manager.tracker.size(), numberOfSegments);

      // We trigger the stop track for all segments, but only the even ones should be cleared.
      for (int i = 0; i < numberOfSegments; i++) {
         manager.stopTrackFor(Flag.PUT_FOR_STATE_TRANSFER, i);
      }

      // Leaving us with 5 segments on tracker.
      assertEquals(manager.tracker.size(), 5);

      // Verify that we are left only with the even ones.
      // This happens because we keep entries to discard the "other way around", we are left with entries telling to
      // discard for state transfers.
      Set<Integer> expectedSegments = IntStream.range(0, numberOfSegments)
            .filter(i -> !isXSiteSegment.test(i)).boxed().collect(Collectors.toSet());
      assertEquals(manager.tracker.keySet(), expectedSegments);
   }
}
