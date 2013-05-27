package org.infinispan.stats.container;


import org.infinispan.util.TimeService;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.stats.container.ExtendedStatistic.*;

/**
 * Thread safe cache statistics that allows multiple writers and reader at the same time.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public final class ConcurrentGlobalContainer {

   private static final int LOCAL_STATS_OFFSET = 1;
   private static final int REMOTE_STATS_OFFSET = LOCAL_STATS_OFFSET + getLocalStatsSize();
   private static final int LOCAL_SIZE = getLocalStatsSize();
   private static final int REMOTE_SIZE = getRemoteStatsSize();
   private static final int TOTAL_SIZE = 1 + LOCAL_SIZE + REMOTE_SIZE;
   private final AtomicBoolean flushing;
   private final BlockingQueue<Mergeable> queue;
   private final TimeService timeService;
   private volatile double[] values;
   private volatile boolean reset;

   public ConcurrentGlobalContainer(TimeService timeService) {
      this.timeService = timeService;
      flushing = new AtomicBoolean(false);
      queue = new LinkedBlockingQueue<Mergeable>();
      values = create();
      values[0] = timeService.time();
   }

   public final void add(ExtendedStatistic stat, double value, boolean local) {
      queue.add(new SingleOperation(local ? getLocalIndex(stat) : getRemoteIndex(stat), value));
      tryFlush();
   }

   public final void merge(double[] toMerge, boolean local) {
      final int expectedSize = local ? LOCAL_SIZE : REMOTE_SIZE;
      final int offset = local ? LOCAL_STATS_OFFSET : REMOTE_STATS_OFFSET;

      if (toMerge.length != expectedSize) {
         throw new IllegalArgumentException("Size mismatch to merge transaction statistic");
      }

      queue.add(new Transaction(toMerge, offset));
      tryFlush();
   }

   public final StatisticsSnapshot getSnapshot() {
      tryFlush();
      return new StatisticsSnapshot(values);
   }

   public final void reset() {
      reset = true;
      tryFlush();
   }

   public static int getLocalIndex(ExtendedStatistic stat) {
      final int index = stat.getLocalIndex();
      if (index == NO_INDEX) {
         throw new IllegalArgumentException("This should never happen. Statistic " + stat + " is not local");
      }
      return LOCAL_STATS_OFFSET + index;
   }

   public static int getRemoteIndex(ExtendedStatistic stat) {
      final int index = stat.getRemoteIndex();
      if (index == NO_INDEX) {
         throw new IllegalArgumentException("This should never happen. Statistic " + stat + " is not remote");
      }
      return REMOTE_STATS_OFFSET + index;
   }

   /**
    * @return TEST ONLY!!
    */
   public final BlockingQueue<?> queue() {
      return queue;
   }

   /**
    * @return TEST ONLY!!
    */
   public final AtomicBoolean flushing() {
      return flushing;
   }

   /**
    * @return TEST ONLY!!
    */
   public final boolean isReset() {
      return reset;
   }

   public final void dumpTo(PrintWriter writer) {
      final double[] snapshot = values;
      writer.println("Global Statistics:");
      for (ExtendedStatistic statistic : ExtendedStatistic.values()) {
         if (statistic.isLocal()) {
            writer.print(statistic.name());
            writer.print(" [local]=");
            writer.println(snapshot[getLocalIndex(statistic)]);
         }
         if (statistic.isRemote()) {
            writer.print(statistic.name());
            writer.print(" [remote]=");
            writer.println(snapshot[getLocalIndex(statistic)]);
         }
      }
      writer.print("LAST_RESET=");
      writer.println(snapshot[0]);
      writer.flush();
   }

   private void tryFlush() {
      if (flushing.compareAndSet(false, true)) {
         flush();
      }
   }

   private void flush() {
      if (reset) {
         values = create();
         queue.clear();
         reset = false;
         values[0] = timeService.time();
         flushing.set(false);
         return;
      }
      final double[] copy = create();
      System.arraycopy(values, 0, copy, 0, copy.length);
      List<Mergeable> drain = new ArrayList<Mergeable>();
      queue.drainTo(drain);
      for (Mergeable mergeable : drain) {
         try {
            mergeable.mergeTo(copy);
         } catch (Throwable throwable) {
            //ignore
         }
      }
      values = copy;
      flushing.set(false);
   }

   private double[] create() {
      return new double[TOTAL_SIZE];
   }

   private interface Mergeable {
      void mergeTo(double[] values);
   }

   private class Transaction implements Mergeable {

      private final double[] toMerge;
      private final int offset;

      private Transaction(double[] toMerge, int offset) {
         this.toMerge = toMerge;
         this.offset = offset;
      }

      @Override
      public void mergeTo(double[] values) {
         for (int i = 0; i < values.length; ++i) {
            values[offset + i] += toMerge[i];
         }
      }
   }

   private class SingleOperation implements Mergeable {

      private final int index;
      private final double value;

      private SingleOperation(int index, double value) {
         this.value = value;
         this.index = index;
      }

      @Override
      public void mergeTo(double[] values) {
         values[index] += value;
      }
   }
}
