package org.infinispan.stats.container;

import org.infinispan.stats.exception.ExtendedStatisticNotFoundException;

import java.io.PrintStream;

/**
 * Common logic for the {@link ExtendedStatisticsContainer}
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public abstract class BaseExtendedStatisticsContainer implements ExtendedStatisticsContainer {

   protected final double[] stats;

   protected BaseExtendedStatisticsContainer(int size) {
      stats = new double[size];
   }

   @Override
   public final void addValue(ExtendedStatistic statistic, double value) {
      int index = getIndex(statistic);
      if (index != ExtendedStatistic.NO_INDEX) {
         stats[index] += value;
      }
   }

   @Override
   public final double getValue(ExtendedStatistic statistic) throws ExtendedStatisticNotFoundException {
      int index = getIndex(statistic);
      if (index != ExtendedStatistic.NO_INDEX) {
         return stats[index];
      }
      throw new ExtendedStatisticNotFoundException(statistic + " not found in " + this);
   }

   @Override
   public final void dumpTo(PrintStream stream) {
      for (ExtendedStatistic statistic : ExtendedStatistic.values()) {
         int index = getIndex(statistic);
         if (index != ExtendedStatistic.NO_INDEX) {
            stream.print(statistic);
            stream.print("=");
            stream.print(stats[index]);
            stream.println();
         }
      }
   }

   @Override
   public final void dumpTo(StringBuilder stringBuilder) {
      for (ExtendedStatistic statistic : ExtendedStatistic.values()) {
         int index = getIndex(statistic);
         if (index != ExtendedStatistic.NO_INDEX) {
            stringBuilder.append(statistic).append("=").append(stats[index])
                  .append(System.getProperty("line.separator"));
         }
      }
   }

   /**
    * @param statistic
    * @return the index of the statistic in this container. It return {@link ExtendedStatistic#NO_INDEX} if the
    *         statistic does not exist in this container
    */
   protected abstract int getIndex(ExtendedStatistic statistic);
}
