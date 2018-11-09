package org.infinispan.commons.stat;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A default {@link SimpleStat} implementation.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public class DefaultSimpleStat implements SimpleStat {

   private final LongAdder count = new LongAdder();
   private final LongAdder sum = new LongAdder();
   private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
   private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);


   @Override
   public void record(long value) {
      updateMin(value);
      updateMax(value);
      sum.add(value);
      count.increment();
   }

   @Override
   public long getMin(long defaultValue) {
      return count.sum() == 0 ? defaultValue : min.get();
   }

   @Override
   public long getMax(long defaultValue) {
      return count.sum() == 0 ? defaultValue : max.get();
   }

   @Override
   public long getAverage(long defaultValue) {
      long counted = count.sum();
      return counted == 0 ? defaultValue : sum.sum() / counted;
   }

   @Override
   public long count() {
      return count.sum();
   }

   private void updateMin(long value) {
      long tmp = min.get();
      while (value < tmp) {
         if (min.compareAndSet(tmp, value)) {
            return;
         }
         tmp = min.get();
      }
   }

   private void updateMax(long value) {
      long tmp = max.get();
      while (value > tmp) {
         if (max.compareAndSet(tmp, value)) {
            return;
         }
         tmp = max.get();
      }
   }
}
