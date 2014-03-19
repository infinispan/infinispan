package org.infinispan.stats.container;

import org.infinispan.stats.ExtendedStatisticNotFoundException;

/**
 * Contains the statistic's values and allows to perform modifications on them.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface ExtendedStatisticsContainer {

   /**
    * it adds the value to the statistic. If the statistic does not exist in this container, it fails silently
    */
   void addValue(ExtendedStatistic statistic, double value);

   /**
    * @return the current value of the statistic
    * @throws ExtendedStatisticNotFoundException
    *          if the statistic was not found in this container
    */
   double getValue(ExtendedStatistic statistic) throws ExtendedStatisticNotFoundException;

   /**
    * it merges in {@code this} the statistic's values in {@code other}. If for some reason the {@code other} cannot be
    * merged, it fails silently
    */
   void mergeTo(ConcurrentGlobalContainer globalContainer);
}
