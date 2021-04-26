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

   private volatile Data data;

   public DefaultSimpleStat() {
      this.data = new Data();
   }

   @Override
   public void record(long value) {
      data.record(value);
   }

   @Override
   public long getMin(long defaultValue) {
      return data.getMin(defaultValue);
   }

   @Override
   public long getMax(long defaultValue) {
      return data.getMax(defaultValue);
   }

   @Override
   public long getAverage(long defaultValue) {
      return data.getAvg(defaultValue);
   }

   @Override
   public long count() {
      return data.count();
   }

   @Override
   public void setTimer(TimerTracker timer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void reset() {
      this.data = new Data();
   }


   private static class Data {
      private final LongAdder count;
      private final LongAdder sum;
      private final AtomicLong min;
      private final AtomicLong max;

      private Data() {
         count = new LongAdder();
         sum = new LongAdder();
         min = new AtomicLong(Long.MAX_VALUE);
         max = new AtomicLong(Long.MIN_VALUE);
      }

      void record(long value) {
         updateMin(value);
         updateMax(value);
         sum.add(value);
         count.increment();
      }

      long getMin(long defaultValue) {
         return count() == 0 ? defaultValue : min.get();
      }

      long getMax(long defaultValue) {
         return count() == 0 ? defaultValue : max.get();
      }

      long getAvg(long defaultValue) {
         long c = count();
         return c == 0 ? defaultValue : sum.sum() / c;
      }

      long count() {
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
}
