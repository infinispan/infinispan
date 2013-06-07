package org.infinispan.stats.container;

import org.infinispan.stats.exception.ExtendedStatisticNotFoundException;

import java.io.PrintStream;

/**
 * Contains the statistic's values and allows to perform modifications on them.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface ExtendedStatisticsContainer {

   /**
    * it adds the value to the statistic. If the statistic does not exist in this container, it fails silently
    *
    * @param statistic
    * @param value
    */
   void addValue(ExtendedStatistic statistic, double value);

   /**
    * @param statistic
    * @return the current value of the statistic
    * @throws ExtendedStatisticNotFoundException
    *          if the statistic was not found in this container
    */
   double getValue(ExtendedStatistic statistic) throws ExtendedStatisticNotFoundException;

   /**
    * it merges in {@code this} the statistic's values in {@code other}. If for some reason the {@code other} cannot be
    * merged, it fails silently
    *
    * @param other
    */
   void merge(ExtendedStatisticsContainer other);

   /**
    * dump the statistics values to the {@link PrintStream}
    *
    * @param stream
    */
   void dumpTo(PrintStream stream);

   /**
    * dump the statistics values to the {@link StringBuilder}
    *
    * @param stringBuilder
    */
   void dumpTo(StringBuilder stringBuilder);

}
