package org.infinispan.stats.container;

/**
 * Container for the statistics corresponding to remote originated transactions. It only knows how to merge from others
 * {@link RemoteExtendedStatisticsContainer}
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class RemoteExtendedStatisticsContainer extends BaseExtendedStatisticsContainer {
   public RemoteExtendedStatisticsContainer() {
      super(ExtendedStatistic.getRemoteStatsSize());
   }

   @Override
   public void mergeTo(ConcurrentGlobalContainer globalContainer) {
      globalContainer.merge(stats, false);
   }

   @Override
   protected int getIndex(ExtendedStatistic statistic) {
      return statistic.getRemoteIndex();
   }
}
