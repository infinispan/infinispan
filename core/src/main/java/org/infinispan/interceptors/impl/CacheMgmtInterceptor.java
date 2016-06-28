package org.infinispan.interceptors.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
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
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.util.TimeService;

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

   private final AtomicLong startNanoseconds = new AtomicLong(0);
   private volatile AtomicLong resetNanoseconds = new AtomicLong(0);
   private Stripe[] stripes = new Stripe[Stripe.STRIPE_COUNT];

   {
      for (int i = 0; i < stripes.length; i++) {
         stripes[i] = new Stripe();
      }
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
   public CompletableFuture<Void> visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      if (!getStatisticsEnabled(command))
         return ctx.continueInvocation();

      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         increment(Stripe.evictionsFieldUpdater, stripeForThread());
         return null;
      });
   }

   @Override
   public final CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   @Override
   public final CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   private CompletableFuture<Void> visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return ctx.continueInvocation();

      long start = timeService.time();
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         Stripe stripe = stripeForThread();
         long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
         if (rv == null) {
            add(Stripe.missTimesFieldUpdater, stripe, intervalMilliseconds);
            increment(Stripe.missesFieldUpdater, stripe);
         } else {
            add(Stripe.hitTimesFieldUpdater, stripe, intervalMilliseconds);
            increment(Stripe.hitsFieldUpdater, stripe);
         }
         return null;
      });
   }

   @SuppressWarnings("unchecked")
   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return ctx.continueInvocation();

      long start = timeService.time();
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
         int requests = ((GetAllCommand) rCommand).getKeys().size();
         int hitCount = 0;
         for (Entry<Object, Object> entry : ((Map<Object, Object>) rv).entrySet()) {
            if (entry.getValue() != null) {
               hitCount++;
            }
         }

         int missCount = requests - hitCount;
         Stripe stripe = stripeForThread();
         if (hitCount > 0) {
            add(Stripe.hitsFieldUpdater, stripe, hitCount);
            add(Stripe.hitTimesFieldUpdater, stripe, intervalMilliseconds * hitCount / requests);
         }
         if (missCount > 0) {
            add(Stripe.missesFieldUpdater, stripe, missCount);
            add(Stripe.missTimesFieldUpdater, stripe, intervalMilliseconds * missCount / requests);
         }
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return ctx.continueInvocation();

      long start = timeService.time();
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         final long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
         final Map<Object, Object> data = ((PutMapCommand) rCommand).getMap();
         if (data != null && !data.isEmpty()) {
            Stripe stripe = stripeForThread();
            add(Stripe.storeTimesFieldUpdater, stripe, intervalMilliseconds);
            add(Stripe.storesFieldUpdater, stripe, data.size());
         }
         return null;
      });
   }

   @Override
   //Map.put(key,value) :: oldValue
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return updateStoreStatistics(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return updateStoreStatistics(ctx, command);
   }

   private CompletableFuture<Void> updateStoreStatistics(InvocationContext ctx, WriteCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return ctx.continueInvocation();

      long start = timeService.time();
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (command.isSuccessful()) {
            long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
            Stripe stripe = stripeForThread();
            add(Stripe.storeTimesFieldUpdater, stripe, intervalMilliseconds);
            increment(Stripe.storesFieldUpdater, stripe);
         }
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return ctx.continueInvocation();

      long start = timeService.time();
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
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
         return null;
      });
   }

   private void increaseRemoveHits(long start) {
      long intervalMilliseconds = timeService.timeDuration(start, TimeUnit.MILLISECONDS);
      Stripe stripe = stripeForThread();
      add(Stripe.removeTimesFieldUpdater, stripe, intervalMilliseconds);
      increment(Stripe.removeHitsFieldUpdater, stripe);
   }

   private void increaseRemoveMisses() {
      increment(Stripe.removeMissesFieldUpdater, stripeForThread());
   }

   @ManagedAttribute(
         description = "Number of cache attribute hits",
         displayName = "Number of cache hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   public long getHits() {
      return get(Stripe.hitsFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache attribute misses",
         displayName = "Number of cache misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getMisses() {
      return get(Stripe.missesFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache removal hits",
         displayName = "Number of cache removal hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getRemoveHits() {
      return get(Stripe.removeHitsFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache removals where keys were not found",
         displayName = "Number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getRemoveMisses() {
      return get(Stripe.removeMissesFieldUpdater);
   }

   @ManagedAttribute(
         description = "number of cache attribute put operations",
         displayName = "Number of cache puts" ,
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getStores() {
      return get(Stripe.storesFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache eviction operations",
         displayName = "Number of cache evictions",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getEvictions() {
      return get(Stripe.evictionsFieldUpdater);
   }

   @ManagedAttribute(
         description = "Percentage hit/(hit+miss) ratio for the cache",
         displayName = "Hit ratio",
         units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public double getHitRatio() {
      long hitsL = get(Stripe.hitsFieldUpdater);
      double total = hitsL + get(Stripe.missesFieldUpdater);
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
      long sum = get(Stripe.storesFieldUpdater);
      if (sum == 0)
         return 0;
      return (((double) (get(Stripe.hitsFieldUpdater) + get(Stripe.missesFieldUpdater)) / (double) sum));
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a read operation on the cache",
         displayName = "Average read time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public long getAverageReadTime() {
      long total = get(Stripe.hitsFieldUpdater) + get(Stripe.missesFieldUpdater);
      if (total == 0)
         return 0;
      return (get(Stripe.hitTimesFieldUpdater) + get(Stripe.missTimesFieldUpdater)) / total;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a write operation in the cache",
         displayName = "Average write time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY
   )
   @SuppressWarnings("unused")
   public long getAverageWriteTime() {
      long sum = get(Stripe.storesFieldUpdater);
      if (sum == 0)
         return 0;
      return (get(Stripe.storeTimesFieldUpdater)) / sum;
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
      return (get(Stripe.removeTimesFieldUpdater)) / removes;
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
      reset(Stripe.hitsFieldUpdater);
      reset(Stripe.missesFieldUpdater);
      reset(Stripe.storesFieldUpdater);
      reset(Stripe.evictionsFieldUpdater);
      reset(Stripe.hitTimesFieldUpdater);
      reset(Stripe.missTimesFieldUpdater);
      reset(Stripe.storeTimesFieldUpdater);
      reset(Stripe.removeHitsFieldUpdater);
      reset(Stripe.removeTimesFieldUpdater);
      reset(Stripe.removeMissesFieldUpdater);
      resetNanoseconds.set(timeService.time());
   }

   private boolean getStatisticsEnabled(FlagAffectedCommand cmd) {
      return super.getStatisticsEnabled() && !cmd.hasFlag(Flag.SKIP_STATISTICS);
   }

   public void addEvictions(long numEvictions) {
      add(Stripe.evictionsFieldUpdater, stripeForThread(), numEvictions);
   }

   private void increment(AtomicLongFieldUpdater<Stripe> updater, Stripe stripe) {
      updater.getAndIncrement(stripe);
   }

   private void add(AtomicLongFieldUpdater<Stripe> updater, Stripe stripe, long delta) {
      updater.getAndAdd(stripe, delta);
   }

   private CacheMgmtInterceptor.Stripe stripeForThread() {
      return stripes[threadIndex()];
   }

   private long get(AtomicLongFieldUpdater<Stripe> updater) {
      long sum = 0;
      for (Stripe stripe : stripes) {
         sum += updater.get(stripe);
      }
      return sum;
   }

   private void reset(AtomicLongFieldUpdater<Stripe> updater) {
      for (Stripe stripe : stripes) {
         updater.set(stripe, 0);
      }
   }

   private int threadIndex() {
      return (int) (Thread.currentThread().getId() & Stripe.STRIPE_MASK);
   }

   @SuppressWarnings("unused")
   private static class Stripe {
      public static final int STRIPE_COUNT =
            (int) (Long.highestOneBit(Runtime.getRuntime().availableProcessors()) << 1);
      public static final int STRIPE_MASK = STRIPE_COUNT - 1;

      private static final AtomicLongFieldUpdater<Stripe> hitTimesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "hitTimes");
      private static final AtomicLongFieldUpdater<Stripe> missTimesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "missTimes");
      private static final AtomicLongFieldUpdater<Stripe> storeTimesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "storeTimes");
      private static final AtomicLongFieldUpdater<Stripe> removeTimesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "removeTimes");
      private static final AtomicLongFieldUpdater<Stripe> hitsFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "hits");
      private static final AtomicLongFieldUpdater<Stripe> missesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "misses");
      private static final AtomicLongFieldUpdater<Stripe> storesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "stores");
      private static final AtomicLongFieldUpdater<Stripe> evictionsFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "evictions");
      private static final AtomicLongFieldUpdater<Stripe> removeHitsFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "removeHits");
      private static final AtomicLongFieldUpdater<Stripe> removeMissesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(Stripe.class, "removeMisses");

      private volatile long hitTimes = 0;
      private volatile long missTimes = 0;
      private volatile long storeTimes = 0;
      private volatile long removeTimes = 0;
      private volatile long hits = 0;
      private volatile long misses = 0;
      private volatile long stores = 0;
      private volatile long evictions = 0;
      private volatile long removeHits = 0;
      private volatile long removeMisses = 0;
   }
}
