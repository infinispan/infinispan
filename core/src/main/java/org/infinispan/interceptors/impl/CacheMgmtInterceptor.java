package org.infinispan.interceptors.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.offheap.OffHeapMemoryAllocator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.StripedCounters;

/**
 * Captures cache management statistics
 *
 * @author Jerry Gauthier
 * @since 9.0
 */
@MBean(objectName = "Statistics", description = "General statistics such as timings, hit/miss ratio, etc.")
public class CacheMgmtInterceptor extends JmxStatsCommandInterceptor {
   private DataContainer dataContainer;
   private TimeService timeService;
   private OffHeapMemoryAllocator allocator;

   private final AtomicLong startNanoseconds = new AtomicLong(0);
   private volatile AtomicLong resetNanoseconds = new AtomicLong(0);
   private StripedCounters<StripeB> counters = new StripedCounters<>(StripeC::new);

   @Inject
   @SuppressWarnings("unused")
   public void setDependencies(DataContainer dataContainer, TimeService timeService, OffHeapMemoryAllocator allocator) {
      this.dataContainer = dataContainer;
      this.timeService = timeService;
      this.allocator = allocator;
   }

   @Start
   public void start() {
      startNanoseconds.set(timeService.time());
      resetNanoseconds.set(startNanoseconds.get());
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      if (!getStatisticsEnabled(command))
         return invokeNext(ctx, command);

      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         counters.increment(StripeB.evictionsFieldUpdater, counters.stripeForCurrentThread());
      });
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   private Object visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         StripeB stripe = counters.stripeForCurrentThread();
         long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
         if (rv == null) {
            counters.add(StripeB.missTimesFieldUpdater, stripe, intervalMilliseconds);
            counters.increment(StripeB.missesFieldUpdater, stripe);
         } else {
            counters.add(StripeB.hitTimesFieldUpdater, stripe, intervalMilliseconds);
            counters.increment(StripeB.hitsFieldUpdater, stripe);
         }
      });
   }

   @SuppressWarnings("unchecked")
   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
         int requests = ((GetAllCommand) rCommand).getKeys().size();
         int hitCount = 0;
         for (Entry<Object, Object> entry : ((Map<Object, Object>) rv).entrySet()) {
            if (entry.getValue() != null) {
               hitCount++;
            }
         }

         int missCount = requests - hitCount;
         StripeB stripe = counters.stripeForCurrentThread();
         if (hitCount > 0) {
            counters.add(StripeB.hitsFieldUpdater, stripe, hitCount);
            counters.add(StripeB.hitTimesFieldUpdater, stripe, intervalMilliseconds * hitCount / requests);
         }
         if (missCount > 0) {
            counters.add(StripeB.missesFieldUpdater, stripe, missCount);
            counters.add(StripeB.missTimesFieldUpdater, stripe, intervalMilliseconds * missCount / requests);
         }
      });
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         final long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
         final Map<Object, Object> data = ((PutMapCommand) rCommand).getMap();
         if (data != null && !data.isEmpty()) {
            StripeB stripe = counters.stripeForCurrentThread();
            counters.add(StripeB.storeTimesFieldUpdater, stripe, intervalMilliseconds);
            counters.add(StripeB.storesFieldUpdater, stripe, data.size());
         }
      });
   }

   @Override
   //Map.put(key,value) :: oldValue
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return updateStoreStatistics(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return updateStoreStatistics(ctx, command);
   }

   private Object updateStoreStatistics(InvocationContext ctx, WriteCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         if (command.isSuccessful()) {
            long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
            StripeB stripe = counters.stripeForCurrentThread();
            counters.add(StripeB.storeTimesFieldUpdater, stripe, intervalMilliseconds);
            counters.increment(StripeB.storesFieldUpdater, stripe);
         }
      });
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         RemoveCommand removeCommand = (RemoveCommand) rCommand;
         if (removeCommand.isConditional()) {
            if (removeCommand.isSuccessful())
               increaseRemoveHits(start);
            else
               increaseRemoveMisses();
         } else {
            if (rv == null)
               increaseRemoveMisses();
            else
               increaseRemoveHits(start);
         }
      });
   }

   private void increaseRemoveHits(long start) {
      long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
      StripeB stripe = counters.stripeForCurrentThread();
      counters.add(StripeB.removeTimesFieldUpdater, stripe, intervalMilliseconds);
      counters.increment(StripeB.removeHitsFieldUpdater, stripe);
   }

   private void increaseRemoveMisses() {
      counters.increment(StripeB.removeMissesFieldUpdater, counters.stripeForCurrentThread());
   }

   @ManagedAttribute(
         description = "Number of cache attribute hits",
         displayName = "Number of cache hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   public long getHits() {
      return counters.get(StripeB.hitsFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache attribute misses",
         displayName = "Number of cache misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getMisses() {
      return counters.get(StripeB.missesFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache removal hits",
         displayName = "Number of cache removal hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getRemoveHits() {
      return counters.get(StripeB.removeHitsFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache removals where keys were not found",
         displayName = "Number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getRemoveMisses() {
      return counters.get(StripeB.removeMissesFieldUpdater);
   }

   @ManagedAttribute(
         description = "number of cache attribute put operations",
         displayName = "Number of cache puts" ,
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getStores() {
      return counters.get(StripeB.storesFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache eviction operations",
         displayName = "Number of cache evictions",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getEvictions() {
      return counters.get(StripeB.evictionsFieldUpdater);
   }

   @ManagedAttribute(
         description = "Percentage hit/(hit+miss) ratio for the cache",
         displayName = "Hit ratio",
         units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public double getHitRatio() {
      long hitsL = counters.get(StripeB.hitsFieldUpdater);
      double total = hitsL + counters.get(StripeB.missesFieldUpdater);
      // The reason for <= is that equality checks
      // should be avoided for floating point numbers.
      if (total <= 0)
         return 0;
      return (hitsL / total);
   }

   @ManagedAttribute(
         description = "read/writes ratio for the cache",
         displayName = "Read/write ratio",
         units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public double getReadWriteRatio() {
      long sum = counters.get(StripeB.storesFieldUpdater);
      if (sum == 0)
         return 0;
      return (((double) (counters.get(StripeB.hitsFieldUpdater) + counters.get(StripeB.missesFieldUpdater)) / (double) sum));
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a read operation on the cache",
         displayName = "Average read time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public long getAverageReadTime() {
      long total = counters.get(StripeB.hitsFieldUpdater) + counters.get(StripeB.missesFieldUpdater);
      if (total == 0)
         return 0;
      return (counters.get(StripeB.hitTimesFieldUpdater) + counters.get(StripeB.missTimesFieldUpdater)) / total;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a write operation in the cache",
         displayName = "Average write time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public long getAverageWriteTime() {
      long sum = counters.get(StripeB.storesFieldUpdater);
      if (sum == 0)
         return 0;
      return (counters.get(StripeB.storeTimesFieldUpdater)) / sum;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a remove operation in the cache",
         displayName = "Average remove time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public long getAverageRemoveTime() {
      long removes = getRemoveHits();
      if (removes == 0)
         return 0;
      return (counters.get(StripeB.removeTimesFieldUpdater)) / removes;
   }

   @ManagedAttribute(
         description = "Number of entries currently in memory including expired entries",
         displayName = "Number of current cache entries",
         displayType = DisplayType.SUMMARY
   )
   public int getNumberOfEntries() {
      return dataContainer.sizeIncludingExpired();
   }

   @ManagedAttribute(
         description = "Amount of memory in bytes allocated in off-heap",
         displayName = "Off-Heap Memory Used",
         displayType = DisplayType.SUMMARY
   )
   public long getOffHeapMemoryUsed() {
      return allocator.getAllocatedAmount();
   }

   @ManagedAttribute(
         description = "Number of seconds since cache started",
         displayName = "Seconds since cache started",
         units = Units.SECONDS,
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getTimeSinceStart() {
      return timeService.timeDuration(startNanoseconds.get(), TimeUnit.SECONDS);
   }

   /**
    * Returns number of seconds since cache started
    *
    * @deprecated use {@link #getTimeSinceStart()} instead.
    * @return number of seconds since cache started
    */
   @ManagedAttribute(
         description = "Number of seconds since cache started",
         displayName = "Seconds since cache started",
         units = Units.SECONDS,
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Deprecated
   public long getElapsedTime() {
      // backward compatibility as we renamed ElapsedTime to TimeSinceStart
      return getTimeSinceStart();
   }

   @ManagedAttribute(
         description = "Number of seconds since the cache statistics were last reset",
         displayName = "Seconds since cache statistics were reset",
         units = Units.SECONDS,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public long getTimeSinceReset() {
      return timeService.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
   }

   @Override
   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset Statistics (Statistics)"
   )
   public void resetStatistics() {
      counters.reset(StripeB.hitsFieldUpdater);
      counters.reset(StripeB.missesFieldUpdater);
      counters.reset(StripeB.storesFieldUpdater);
      counters.reset(StripeB.evictionsFieldUpdater);
      counters.reset(StripeB.hitTimesFieldUpdater);
      counters.reset(StripeB.missTimesFieldUpdater);
      counters.reset(StripeB.storeTimesFieldUpdater);
      counters.reset(StripeB.removeHitsFieldUpdater);
      counters.reset(StripeB.removeTimesFieldUpdater);
      counters.reset(StripeB.removeMissesFieldUpdater);
      resetNanoseconds.set(timeService.time());
   }

   private boolean getStatisticsEnabled(FlagAffectedCommand cmd) {
      return super.getStatisticsEnabled() && !cmd.hasAnyFlag(FlagBitSets.SKIP_STATISTICS);
   }

   public void addEvictions(long numEvictions) {
      counters.add(StripeB.evictionsFieldUpdater, counters.stripeForCurrentThread(), numEvictions);
   }

   @SuppressWarnings("unused")
   private static class StripeA {
      private long slack1, slack2, slack3, slack4, slack5, slack6, slack7, slack8;
   }

   @SuppressWarnings({"unused", "VolatileLongOrDoubleField"})
   private static class StripeB extends StripeA {
      static final AtomicLongFieldUpdater<StripeB> hitTimesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "hitTimes");
      static final AtomicLongFieldUpdater<StripeB> missTimesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "missTimes");
      static final AtomicLongFieldUpdater<StripeB> storeTimesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "storeTimes");
      static final AtomicLongFieldUpdater<StripeB> removeHitsFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "removeHits");
      static final AtomicLongFieldUpdater<StripeB> removeMissesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "removeMisses");
      static final AtomicLongFieldUpdater<StripeB> storesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "stores");
      static final AtomicLongFieldUpdater<StripeB> evictionsFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "evictions");
      static final AtomicLongFieldUpdater<StripeB> missesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "misses");
      static final AtomicLongFieldUpdater<StripeB> hitsFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "hits");
      static final AtomicLongFieldUpdater<StripeB> removeTimesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "removeTimes");

      private volatile long hits = 0;
      private volatile long hitTimes = 0;
      private volatile long misses = 0;
      private volatile long missTimes = 0;
      private volatile long stores = 0;
      private volatile long storeTimes = 0;
      private volatile long evictions = 0;
      private volatile long removeHits = 0;
      private volatile long removeMisses = 0;
      private volatile long removeTimes = 0;
   }

   @SuppressWarnings("unused")
   private static class StripeC extends StripeB {
      private long slack1, slack2, slack3, slack4, slack5, slack6, slack7, slack8;
   }
}
