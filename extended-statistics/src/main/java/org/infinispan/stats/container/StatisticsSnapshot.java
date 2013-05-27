package org.infinispan.stats.container;

/**
 * A Statistic Snapshot;
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class StatisticsSnapshot {

   private final double[] snapshot;

   public StatisticsSnapshot(double[] snapshot) {
      this.snapshot = snapshot;
   }

   public final double getRemote(ExtendedStatistic stat) {
      return snapshot[ConcurrentGlobalContainer.getRemoteIndex(stat)];
   }

   public final double getLocal(ExtendedStatistic stat) {
      return snapshot[ConcurrentGlobalContainer.getLocalIndex(stat)];
   }

   public final long getLastResetTime() {
      return (long) snapshot[0];
   }
}
