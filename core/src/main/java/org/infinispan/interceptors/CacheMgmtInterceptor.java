package org.infinispan.interceptors;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.infinispan.commons.util.concurrent.jdk8backported.LongAdder;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Captures cache management statistics
 *
 * @author Jerry Gauthier
 * @since 4.0
 */
@MBean(objectName = "Statistics", description = "General statistics such as timings, hit/miss ratio, etc.")
public class CacheMgmtInterceptor extends JmxStatsCommandInterceptor {
   private final LongAdder hitTimes = new LongAdder();
   private final LongAdder missTimes = new LongAdder();
   private final LongAdder storeTimes = new LongAdder();
   private final LongAdder removeTimes = new LongAdder();
   private final LongAdder hits = new LongAdder();
   private final LongAdder misses = new LongAdder();
   private final LongAdder stores = new LongAdder();
   private final LongAdder evictions = new LongAdder();
   private final AtomicLong startNanoseconds = new AtomicLong(0);
   private final AtomicLong resetNanoseconds = new AtomicLong(0);
   private final LongAdder removeHits = new LongAdder();
   private final LongAdder removeMisses = new LongAdder();

   private DataContainer dataContainer;
   private TimeService timeService;

   private static final Log log = LogFactory.getLog(CacheMgmtInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   @SuppressWarnings("unused")
   public void setDependencies(DataContainer dataContainer, TimeService timeService) {
      this.dataContainer = dataContainer;
      this.timeService = timeService;
   }

   @Start
   public void start() {
      startNanoseconds.set(timeService.time());
      resetNanoseconds.set(startNanoseconds.get());
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (getStatisticsEnabled(command))
         evictions.increment();

      return returnValue;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      long start = 0;
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (statisticsEnabled)
         start = timeService.time();

      Object retval = invokeNextInterceptor(ctx, command);

      if (statisticsEnabled) {
         long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
         if (ctx.isOriginLocal()) {
            if (retval == null) {
               missTimes.add(intervalMilliseconds);
               misses.increment();
            } else {
               hitTimes.add(intervalMilliseconds);
               hits.increment();
            }
         }
      }

      return retval;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      long start = 0;
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (statisticsEnabled)
         start = timeService.time();

      final Object retval = invokeNextInterceptor(ctx, command);

      if (statisticsEnabled) {
         final long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
         final Map<Object, Object> data = command.getMap();
         if (data != null && ctx.isOriginLocal() && !data.isEmpty()) {
            storeTimes.add(intervalMilliseconds);
            stores.add(data.size());
         }
      }

      return retval;
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
      long start = 0;
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (statisticsEnabled)
         start = timeService.time();

      Object retval = invokeNextInterceptor(ctx, command);

      if (statisticsEnabled && ctx.isOriginLocal() && command.isSuccessful()) {
         long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
         storeTimes.add(intervalMilliseconds);
         stores.increment();
      }

      return retval;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      long start = 0;
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (statisticsEnabled)
         start = timeService.time();

      Object retval = invokeNextInterceptor(ctx, command);

      if (statisticsEnabled && ctx.isOriginLocal()) {
         if (command.isConditional()) {
            if (command.isSuccessful())
               increaseRemoveHits(start);
            else
               increaseRemoveMisses();
         } else {
            if (retval == null)
               increaseRemoveMisses();
            else
               increaseRemoveHits(start);
         }
      }

      return retval;
   }

   private void increaseRemoveHits(long start) {
      long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
      removeTimes.add(intervalMilliseconds);
      removeHits.increment();
   }

   private void increaseRemoveMisses() {
      removeMisses.increment();
   }

   @ManagedAttribute(
         description = "Number of cache attribute hits",
         displayName = "Number of cache hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   public long getHits() {
      return hits.sum();
   }

   @ManagedAttribute(
         description = "Number of cache attribute misses",
         displayName = "Number of cache misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getMisses() {
      return misses.sum();
   }

   @ManagedAttribute(
         description = "Number of cache removal hits",
         displayName = "Number of cache removal hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getRemoveHits() {
      return removeHits.sum();
   }

   @ManagedAttribute(
         description = "Number of cache removals where keys were not found",
         displayName = "Number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getRemoveMisses() {
      return removeMisses.sum();
   }

   @ManagedAttribute(
         description = "number of cache attribute put operations",
         displayName = "Number of cache puts" ,
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getStores() {
      return stores.sum();
   }

   @ManagedAttribute(
         description = "Number of cache eviction operations",
         displayName = "Number of cache evictions",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getEvictions() {
      return evictions.sum();
   }

   @ManagedAttribute(
         description = "Percentage hit/(hit+miss) ratio for the cache",
         displayName = "Hit ratio",
         units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public double getHitRatio() {
      long hitsL = hits.sum();
      double total = hitsL + misses.sum();
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
      if (stores.sum() == 0)
         return 0;
      return (((double) (hits.sum() + misses.sum()) / (double) stores.sum()));
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a read operation on the cache",
         displayName = "Average read time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public long getAverageReadTime() {
      long total = hits.sum() + misses.sum();
      if (total == 0)
         return 0;
      return (hitTimes.sum() + missTimes.sum()) / total;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a write operation in the cache",
         displayName = "Average write time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public long getAverageWriteTime() {
      if (stores.sum() == 0)
         return 0;
      return (storeTimes.sum()) / stores.sum();
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
      return (removeTimes.sum()) / removes;
   }

   @ManagedAttribute(
         description = "Number of entries currently in the cache",
         displayName = "Number of current cache entries",
         displayType = DisplayType.SUMMARY
   )
   public int getNumberOfEntries() {
      return dataContainer.size();
   }

   @ManagedAttribute(
         description = "Number of seconds since cache started",
         displayName = "Seconds since cache started",
         units = Units.SECONDS,
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getElapsedTime() {
      return timeService.timeDuration(startNanoseconds.get(), TimeUnit.SECONDS);
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
      hits.reset();
      misses.reset();
      stores.reset();
      evictions.reset();
      hitTimes.reset();
      missTimes.reset();
      storeTimes.reset();
      removeHits.reset();
      removeTimes.reset();
      removeMisses.reset();
      resetNanoseconds.set(timeService.time());
   }

   private boolean getStatisticsEnabled(FlagAffectedCommand cmd) {
      return super.getStatisticsEnabled() && !cmd.hasFlag(Flag.SKIP_STATISTICS);
   }

}

