package org.infinispan.stats.container;

/**
 * Container for the statistics corresponding to local originated transactions. It only knows how to merge from others
 * {@link LocalExtendedStatisticsContainer}
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class LocalExtendedStatisticsContainer extends BaseExtendedStatisticsContainer {
   public LocalExtendedStatisticsContainer() {
      super(ExtendedStatistic.getLocalStatsSize());
   }

   @Override
   public final void mergeTo(ConcurrentGlobalContainer globalContainer) {
      globalContainer.merge(stats, true);
   }

   @Override
   public final String toString() {
      return "LocalExtendedStatisticsContainer";
   }

   @Override
   protected final int getIndex(ExtendedStatistic statistic) {
      return statistic.getLocalIndex();
   }

}
