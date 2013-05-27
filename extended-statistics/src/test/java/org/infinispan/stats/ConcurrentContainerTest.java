package org.infinispan.stats;

import org.infinispan.stats.container.ConcurrentGlobalContainer;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.container.StatisticsSnapshot;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.fail;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "stats.ConcurrentContainerTest")
public class ConcurrentContainerTest {

   private static final TimeService TIME_SERVICE = new DefaultTimeService();

   public void testIsolationWithTransactionMerge() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer(TIME_SERVICE);
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);

      snapshots.add(globalContainer.getSnapshot());
      int localIndex;
      int remoteIndex;

      LocalTransactionStatistics localTransactionStatistics = new LocalTransactionStatistics(false, TIME_SERVICE);

      localIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            localTransactionStatistics.addValue(stats, localIndex++);
         }
      }

      localTransactionStatistics.flushTo(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, (double) localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D), stats, false);
         }
      }

      RemoteTransactionStatistics remoteTransactionStatistics = new RemoteTransactionStatistics(TIME_SERVICE);

      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isRemote()) {
            remoteTransactionStatistics.addValue(stats, remoteIndex++);
         }
      }

      remoteTransactionStatistics.flushTo(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, (double) localIndex, (double) localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D, (double) remoteIndex), stats, false);
            remoteIndex++;
         }
      }

      assertFinalState(globalContainer);
   }

   public void testIsolationWithSingleActionMerge() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer(TIME_SERVICE);
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);
      snapshots.add(globalContainer.getSnapshot());

      //two random stats, one local and one remote
      final ExtendedStatistic localStat = ExtendedStatistic.PREPARE_COMMAND_SIZE;
      final ExtendedStatistic remoteStat = ExtendedStatistic.NUM_COMMITTED_WR_TX;

      Assert.assertTrue(localStat.isLocal());
      Assert.assertTrue(remoteStat.isRemote());

      globalContainer.add(localStat, 10, true);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0D, 10D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D), remoteStat, false);


      globalContainer.add(remoteStat, 20, false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0D, 10D, 10D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D, 20D), remoteStat, false);

      try {
         globalContainer.add(localStat, 30, false);
         fail("Expected exception");
      } catch (Exception e) {
         //expected
      }

      assertSnapshotValues(snapshots, Arrays.asList(0D, 10D, 10D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D, 20D), remoteStat, false);

      assertFinalState(globalContainer);
   }

   public void testIsolationWithReset() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer(TIME_SERVICE);
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);

      snapshots.add(globalContainer.getSnapshot());
      int localIndex;
      int remoteIndex;

      LocalTransactionStatistics localTransactionStatistics = new LocalTransactionStatistics(false, TIME_SERVICE);
      RemoteTransactionStatistics remoteTransactionStatistics = new RemoteTransactionStatistics(TIME_SERVICE);

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            localTransactionStatistics.addValue(stats, localIndex++);
         }
         if (stats.isRemote()) {
            remoteTransactionStatistics.addValue(stats, remoteIndex++);
         }
      }

      localTransactionStatistics.flushTo(globalContainer);
      remoteTransactionStatistics.flushTo(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, (double) localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, (double) remoteIndex), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.reset();

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, (double) localIndex, 0D), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, (double) remoteIndex, 0D), stats, false);
            remoteIndex++;
         }
      }

      localTransactionStatistics.flushTo(globalContainer);
      remoteTransactionStatistics.flushTo(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, (double) localIndex, 0D, (double) localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, (double) remoteIndex, 0D, (double) remoteIndex), stats, false);
            remoteIndex++;
         }
      }

      assertFinalState(globalContainer);
   }

   public void testIsolationWithResetMerge() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer(TIME_SERVICE);
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);
      snapshots.add(globalContainer.getSnapshot());

      //two random stats, one local and one remote
      final ExtendedStatistic localStat = ExtendedStatistic.PREPARE_COMMAND_SIZE;
      final ExtendedStatistic remoteStat = ExtendedStatistic.NUM_COMMITTED_WR_TX;

      Assert.assertTrue(localStat.isLocal());
      Assert.assertTrue(remoteStat.isRemote());

      globalContainer.add(localStat, 10, true);
      globalContainer.add(remoteStat, 20, false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0D, 10D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 20D), remoteStat, false);

      globalContainer.reset();
      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0D, 10D, 0D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 20D, 0D), remoteStat, false);

      globalContainer.add(localStat, 10, true);
      globalContainer.add(remoteStat, 20, false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0D, 10D, 0D, 10D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 20D, 0D, 20D), remoteStat, false);

      assertFinalState(globalContainer);
   }

   public void testIsolationWithEnqueueAndResetTransaction() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer(TIME_SERVICE);
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);

      snapshots.add(globalContainer.getSnapshot());
      int localIndex;
      int remoteIndex;

      LocalTransactionStatistics localTransactionStatistics = new LocalTransactionStatistics(false, TIME_SERVICE);
      RemoteTransactionStatistics remoteTransactionStatistics = new RemoteTransactionStatistics(TIME_SERVICE);

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            localTransactionStatistics.addValue(stats, localIndex++);
         }
         if (stats.isRemote()) {
            remoteTransactionStatistics.addValue(stats, remoteIndex++);
         }
      }

      //all the stuff is enqueued
      globalContainer.flushing().set(true);

      localTransactionStatistics.flushTo(globalContainer);
      remoteTransactionStatistics.flushTo(globalContainer);
      localTransactionStatistics.flushTo(globalContainer);
      remoteTransactionStatistics.flushTo(globalContainer);
      localTransactionStatistics.flushTo(globalContainer);
      remoteTransactionStatistics.flushTo(globalContainer);

      Assert.assertEquals(globalContainer.queue().size(), 6);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.flushing().set(false);

      //this should flush pending statistics
      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D, 3D * localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D, 3D * remoteIndex), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.reset();
      snapshots.clear();
      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.flushing().set(true);

      localTransactionStatistics.flushTo(globalContainer);
      remoteTransactionStatistics.flushTo(globalContainer);
      localTransactionStatistics.flushTo(globalContainer);
      remoteTransactionStatistics.flushTo(globalContainer);
      localTransactionStatistics.flushTo(globalContainer);

      globalContainer.reset();

      remoteTransactionStatistics.flushTo(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      Assert.assertTrue(globalContainer.isReset());
      Assert.assertEquals(globalContainer.queue().size(), 6);

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.flushing().set(false);
      snapshots.add(globalContainer.getSnapshot());
      Assert.assertFalse(globalContainer.isReset());

      localIndex = 0;
      remoteIndex = 0;
      for (ExtendedStatistic stats : ExtendedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D, 0D), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Double>asList(0D, 0D, 0D), stats, false);
            remoteIndex++;
         }
      }

      assertFinalState(globalContainer);
   }

   public void testIsolationWithEnqueueAndResetSingleAction() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer(TIME_SERVICE);
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);
      snapshots.add(globalContainer.getSnapshot());

      //two random stats, one local and one remote
      final ExtendedStatistic localStat = ExtendedStatistic.PREPARE_COMMAND_SIZE;
      final ExtendedStatistic remoteStat = ExtendedStatistic.NUM_COMMITTED_WR_TX;

      Assert.assertTrue(localStat.isLocal());
      Assert.assertTrue(remoteStat.isRemote());

      globalContainer.flushing().set(true);

      globalContainer.add(localStat, 10, true);
      globalContainer.add(remoteStat, 20, false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D), remoteStat, false);

      Assert.assertEquals(globalContainer.queue().size(), 2);
      globalContainer.flushing().set(false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D, 10D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D, 20D), remoteStat, false);

      globalContainer.reset();
      snapshots.clear();
      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D), remoteStat, false);

      globalContainer.flushing().set(true);

      globalContainer.add(localStat, 10, true);
      globalContainer.add(remoteStat, 20, false);
      globalContainer.reset();

      snapshots.add(globalContainer.getSnapshot());
      Assert.assertTrue(globalContainer.isReset());
      Assert.assertEquals(globalContainer.queue().size(), 2);

      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D), remoteStat, false);

      globalContainer.flushing().set(false);

      snapshots.add(globalContainer.getSnapshot());
      Assert.assertFalse(globalContainer.isReset());
      Assert.assertTrue(globalContainer.queue().isEmpty());

      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D, 0D), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0D, 0D, 0D), remoteStat, false);

      assertFinalState(globalContainer);
   }

   private void assertSnapshotValues(List<StatisticsSnapshot> snapshots, List<Double> expected, ExtendedStatistic stat, boolean local) {
      Assert.assertEquals(snapshots.size(), expected.size());
      for (int i = 0; i < snapshots.size(); ++i) {
         if (local) {
            Assert.assertEquals(expected.get(i), snapshots.get(i).getLocal(stat));
         } else {
            Assert.assertEquals(expected.get(i), snapshots.get(i).getRemote(stat));
         }
      }
   }

   private void assertFinalState(ConcurrentGlobalContainer globalContainer) {
      Assert.assertTrue(globalContainer.queue().isEmpty());
      Assert.assertFalse(globalContainer.isReset());
      Assert.assertFalse(globalContainer.flushing().get());
   }
}
