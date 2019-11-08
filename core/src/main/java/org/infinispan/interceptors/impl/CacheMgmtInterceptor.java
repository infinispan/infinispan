package org.infinispan.interceptors.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.functional.AbstractWriteManyCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.concurrent.StripedCounters;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.offheap.OffHeapMemoryAllocator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.functional.impl.StatsEnvelope;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.topology.CacheTopology;

/**
 * Captures cache management statistics.
 *
 * @author Jerry Gauthier
 * @since 9.0
 */
@MBean(objectName = "Statistics", description = "General statistics such as timings, hit/miss ratio, etc.")
public final class CacheMgmtInterceptor extends JmxStatsCommandInterceptor {
   @Inject ComponentRef<AdvancedCache> cache;
   @Inject InternalDataContainer dataContainer;
   @Inject TimeService timeService;
   @Inject OffHeapMemoryAllocator allocator;
   @Inject ComponentRegistry componentRegistry;

   private final AtomicLong startNanoseconds = new AtomicLong(0);
   private final AtomicLong resetNanoseconds = new AtomicLong(0);
   private final StripedCounters<StripeB> counters = new StripedCounters<>(StripeC::new);

   @Start
   public void start() {
      startNanoseconds.set(timeService.time());
      resetNanoseconds.set(startNanoseconds.get());
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // This is just here to notify that evictions are counted in the ClusteringDependentLogic via NotifyHelper and
      // EvictionManager
      return super.visitEvictCommand(ctx, command);
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) {
      return visitDataReadCommand(ctx, command);
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) {
      return visitDataReadCommand(ctx, command);
   }

   public void addDataRead(boolean foundValue, long timeNanoSeconds) {
      StripeB stripe = counters.stripeForCurrentThread();
      if (foundValue) {
         counters.add(StripeB.hitTimesFieldUpdater, stripe, timeNanoSeconds);
         counters.increment(StripeB.hitsFieldUpdater, stripe);
      } else {
         counters.add(StripeB.missTimesFieldUpdater, stripe, timeNanoSeconds);
         counters.increment(StripeB.missesFieldUpdater, stripe);
      }
   }

   private Object visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command) {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command,
            (rCtx, rCommand, rv, t) -> addDataRead(rv != null, timeService.timeDuration(start, TimeUnit.NANOSECONDS)));
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         long intervalNanoseconds = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
         int requests = rCommand.getKeys().size();
         int hitCount = 0;
         if (t == null) {
            for (Entry<?, ?> entry : ((Map<?, ?>) rv).entrySet()) {
               if (entry.getValue() != null) {
                  hitCount++;
               }
            }
         }

         int missCount = requests - hitCount;
         StripeB stripe = counters.stripeForCurrentThread();
         if (hitCount > 0) {
            counters.add(StripeB.hitsFieldUpdater, stripe, hitCount);
            counters.add(StripeB.hitTimesFieldUpdater, stripe, intervalNanoseconds * hitCount / requests);
         }
         if (missCount > 0) {
            counters.add(StripeB.missesFieldUpdater, stripe, missCount);
            counters.add(StripeB.missTimesFieldUpdater, stripe, intervalNanoseconds * missCount / requests);
         }
      });
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         final long intervalNanoseconds = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
         final Map<Object, Object> data = rCommand.getMap();
         if (data != null && !data.isEmpty()) {
            StripeB stripe = counters.stripeForCurrentThread();
            counters.add(StripeB.storeTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.add(StripeB.storesFieldUpdater, stripe, data.size());
         }
      });
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return updateStoreStatistics(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      return updateStoreStatistics(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         if (rv == null && rCommand.isSuccessful()) {
            increaseRemoveMisses();
         } else if (rCommand.isSuccessful()) {
            long intervalNanos = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
            StripeB stripe = counters.stripeForCurrentThread();
            counters.add(StripeB.storeTimesFieldUpdater, stripe, intervalNanos);
            counters.increment(StripeB.storesFieldUpdater, stripe);
         }
      });
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) {
      return updateStoreStatistics(ctx, command);
   }

   private Object updateStoreStatistics(InvocationContext ctx, WriteCommand command) {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         if (rCommand.isSuccessful()) {
            long intervalNanoseconds = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
            StripeB stripe = counters.stripeForCurrentThread();
            counters.add(StripeB.storeTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.increment(StripeB.storesFieldUpdater, stripe);
         }
      });
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) {
      if (!ctx.isOriginLocal() || command.hasAnyFlag(FlagBitSets.SKIP_STATISTICS))
         return invokeNext(ctx, command);

      if (!getStatisticsEnabled())
         return invokeNextThenApply(ctx, command, StatsEnvelope::unpack);

      long start = timeService.time();
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         long intervalNanoseconds = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
         StripeB stripe = counters.stripeForCurrentThread();
         StatsEnvelope envelope = (StatsEnvelope) rv;
         if (envelope.isMiss()) {
            counters.add(StripeB.missTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.increment(StripeB.missesFieldUpdater, stripe);
         } else if (envelope.isHit()){
            counters.add(StripeB.hitTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.increment(StripeB.hitsFieldUpdater, stripe);
         }
         return envelope.value();
      });
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) {
      if (!ctx.isOriginLocal() || command.hasAnyFlag(FlagBitSets.SKIP_STATISTICS))
         return invokeNext(ctx, command);

      if (!getStatisticsEnabled())
         return invokeNextThenApply(ctx, command, StatsEnvelope::unpackStream);

      long start = timeService.time();
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         long intervalNanoseconds = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
         StripeB stripe = counters.stripeForCurrentThread();
         ByRef.Integer hitCount = new ByRef.Integer(0);
         ByRef.Integer missCount = new ByRef.Integer(0);
         int numResults = rCommand.getKeys().size();
         Collection<Object> retvals = new ArrayList<>(numResults);
         ((Stream<StatsEnvelope<Object>>) rv).forEach(e -> {
            if (e.isHit()) hitCount.inc();
            if (e.isMiss()) missCount.inc();
            retvals.add(e.value());
         });
         if (missCount.get() > 0) {
            counters.add(StripeB.missTimesFieldUpdater, stripe, missCount.get() * intervalNanoseconds / numResults);
            counters.add(StripeB.missesFieldUpdater, stripe, missCount.get());
         }
         if (hitCount.get() > 0) {
            counters.add(StripeB.hitTimesFieldUpdater, stripe, hitCount.get() * intervalNanoseconds / numResults);
            counters.add(StripeB.hitsFieldUpdater, stripe, hitCount.get());
         }
         return retvals.stream();
      });
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) {
      return updateStatisticsWriteOnly(ctx, command);
   }

   private Object updateStatisticsWriteOnly(InvocationContext ctx, AbstractDataCommand command) {
      if (!ctx.isOriginLocal() || command.hasAnyFlag(FlagBitSets.SKIP_STATISTICS)) {
         return invokeNext(ctx, command);
      }
      if (!getStatisticsEnabled())
         return invokeNextThenApply(ctx, command, StatsEnvelope::unpack);

      long start = timeService.time();
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         long intervalNanoseconds = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
         StripeB stripe = counters.stripeForCurrentThread();
         StatsEnvelope<?> envelope = (StatsEnvelope<?>) rv;
         if (envelope.isDelete()) {
            counters.add(StripeB.removeTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.increment(StripeB.removeHitsFieldUpdater, stripe);
         } else if ((envelope.flags() & (StatsEnvelope.CREATE | StatsEnvelope.UPDATE)) != 0) {
            counters.add(StripeB.storeTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.increment(StripeB.storesFieldUpdater, stripe);
         }
         assert envelope.value() == null;
         return null;
      });
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) {
      return updateStatisticsReadWrite(ctx, command);
   }

   private Object updateStatisticsReadWrite(InvocationContext ctx, AbstractDataCommand command) {
      if (!ctx.isOriginLocal() || command.hasAnyFlag(FlagBitSets.SKIP_STATISTICS)) {
         return invokeNext(ctx, command);
      }

      if (!getStatisticsEnabled())
         return invokeNextThenApply(ctx, command, StatsEnvelope::unpack);

      long start = timeService.time();
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         long intervalNanoseconds = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
         StripeB stripe = counters.stripeForCurrentThread();
         StatsEnvelope<?> envelope = (StatsEnvelope<?>) rv;
         if (envelope.isDelete()) {
            counters.add(StripeB.removeTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.increment(StripeB.removeHitsFieldUpdater, stripe);
         } else if ((envelope.flags() & (StatsEnvelope.CREATE | StatsEnvelope.UPDATE)) != 0) {
            counters.add(StripeB.storeTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.increment(StripeB.storesFieldUpdater, stripe);
         }
         if (envelope.isHit()) {
            counters.add(StripeB.hitTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.increment(StripeB.hitsFieldUpdater, stripe);
         } else if (envelope.isMiss()) {
            counters.add(StripeB.missTimesFieldUpdater, stripe, intervalNanoseconds);
            counters.increment(StripeB.missesFieldUpdater, stripe);
         }
         return envelope.value();
      });
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) {
      return updateStatisticsReadWrite(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) {
      return updateStatisticsWriteOnly(ctx, command);
   }

   // TODO: WriteOnlyManyCommand and WriteOnlyManyEntriesCommand not implemented as the rest of stack
   // does not pass the return value.

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) {
      return updateStatisticsReadWrite(ctx, command);
   }

   private Object updateStatisticsReadWrite(InvocationContext ctx, AbstractWriteManyCommand command) {
      if (!ctx.isOriginLocal() || command.hasAnyFlag(FlagBitSets.SKIP_STATISTICS)) {
         return invokeNext(ctx, command);
      }

      if (!getStatisticsEnabled())
         return invokeNextThenApply(ctx, command, StatsEnvelope::unpackCollection);

      long start = timeService.time();
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         long intervalNanoseconds = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
         StripeB stripe = counters.stripeForCurrentThread();

         int hits = 0;
         int misses = 0;
         int stores = 0;
         int removals = 0;
         int numResults = rCommand.getAffectedKeys().size();
         List<Object> results = new ArrayList<>(numResults);
         for(StatsEnvelope<?> envelope : ((Collection<StatsEnvelope<?>>) rv)) {
            if (envelope.isDelete()) {
               removals++;
            } else if ((envelope.flags() & (StatsEnvelope.CREATE | StatsEnvelope.UPDATE)) != 0) {
               stores++;
            }
            if (envelope.isHit()) {
               hits++;
            } else if (envelope.isMiss()) {
               misses++;
            }
            results.add(envelope.value());
         }
         if (removals > 0) {
            counters.add(StripeB.removeTimesFieldUpdater, stripe, removals * intervalNanoseconds / numResults);
            counters.add(StripeB.removeHitsFieldUpdater, stripe, removals);
         }
         if (stores > 0) {
            counters.add(StripeB.storeTimesFieldUpdater, stripe, stores * intervalNanoseconds / numResults);
            counters.add(StripeB.storesFieldUpdater, stripe, stores);
         }
         if (misses > 0) {
            counters.add(StripeB.missTimesFieldUpdater, stripe, misses * intervalNanoseconds / numResults);
            counters.add(StripeB.missesFieldUpdater, stripe, misses);
         }
         if (hits > 0) {
            counters.add(StripeB.hitTimesFieldUpdater, stripe, hits * intervalNanoseconds / numResults);
            counters.add(StripeB.hitsFieldUpdater, stripe, hits);
         }
         return results;
      });
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) {
      return updateStatisticsReadWrite(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      boolean statisticsEnabled = getStatisticsEnabled(command);
      if (!statisticsEnabled || !ctx.isOriginLocal())
         return invokeNext(ctx, command);

      long start = timeService.time();
      return invokeNextAndFinally(ctx, command, (rCtx, removeCommand, rv, t) -> {
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
      long intervalNanoseconds = timeService.timeDuration(start, TimeUnit.NANOSECONDS);
      StripeB stripe = counters.stripeForCurrentThread();
      counters.add(StripeB.removeTimesFieldUpdater, stripe, intervalNanoseconds);
      counters.increment(StripeB.removeHitsFieldUpdater, stripe);
   }

   private void increaseRemoveMisses() {
      counters.increment(StripeB.removeMissesFieldUpdater, counters.stripeForCurrentThread());
   }

   @ManagedAttribute(
         description = "Number of cache attribute hits",
         displayName = "Number of cache hits",
         measurementType = MeasurementType.TRENDSUP)
   public long getHits() {
      return counters.get(StripeB.hitsFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache attribute misses",
         displayName = "Number of cache misses",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getMisses() {
      return counters.get(StripeB.missesFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache removal hits",
         displayName = "Number of cache removal hits",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getRemoveHits() {
      return counters.get(StripeB.removeHitsFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache removals where keys were not found",
         displayName = "Number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getRemoveMisses() {
      return counters.get(StripeB.removeMissesFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache attribute put operations",
         displayName = "Number of cache puts" ,
         measurementType = MeasurementType.TRENDSUP
   )
   public long getStores() {
      return counters.get(StripeB.storesFieldUpdater);
   }

   @ManagedAttribute(
         description = "Number of cache eviction operations",
         displayName = "Number of cache evictions",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getEvictions() {
      return counters.get(StripeB.evictionsFieldUpdater);
   }

   @ManagedAttribute(
         description = "Percentage hit/(hit+miss) ratio for the cache",
         displayName = "Hit ratio",
         units = Units.PERCENTAGE
   )
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
         description = "Read/writes ratio for the cache",
         displayName = "Read/write ratio",
         units = Units.PERCENTAGE
   )
   public double getReadWriteRatio() {
      long sum = counters.get(StripeB.storesFieldUpdater);
      if (sum == 0)
         return 0;
      return (double) (counters.get(StripeB.hitsFieldUpdater) + counters.get(StripeB.missesFieldUpdater)) / (double) sum;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a read operation on the cache",
         displayName = "Average read time",
         units = Units.MILLISECONDS
   )
   public long getAverageReadTime() {
      long total = counters.get(StripeB.hitsFieldUpdater) + counters.get(StripeB.missesFieldUpdater);
      if (total == 0)
         return 0;
      total = (counters.get(StripeB.hitTimesFieldUpdater) + counters.get(StripeB.missTimesFieldUpdater)) / total;
      return TimeUnit.NANOSECONDS.toMillis(total);
   }

   @ManagedAttribute(
         description = "Average number of nanoseconds for a read operation on the cache",
         displayName = "Average read time",
         units = Units.NANOSECONDS
   )
   public long getAverageReadTimeNanos() {
      long total = counters.get(StripeB.hitsFieldUpdater) + counters.get(StripeB.missesFieldUpdater);
      if (total == 0)
         return 0;
      return (counters.get(StripeB.hitTimesFieldUpdater) + counters.get(StripeB.missTimesFieldUpdater)) / total;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a write operation in the cache",
         displayName = "Average write time",
         units = Units.MILLISECONDS
   )
   public long getAverageWriteTime() {
      long sum = counters.get(StripeB.storesFieldUpdater);
      if (sum == 0)
         return 0;
      return TimeUnit.NANOSECONDS.toMillis(counters.get(StripeB.storeTimesFieldUpdater) / sum);
   }

   @ManagedAttribute(
         description = "Average number of nanoseconds for a write operation in the cache",
         displayName = "Average write time",
         units = Units.NANOSECONDS
   )
   public long getAverageWriteTimeNanos() {
      long sum = counters.get(StripeB.storesFieldUpdater);
      if (sum == 0)
         return 0;
      return counters.get(StripeB.storeTimesFieldUpdater) / sum;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a remove operation in the cache",
         displayName = "Average remove time",
         units = Units.MILLISECONDS
   )
   public long getAverageRemoveTime() {
      long removes = getRemoveHits();
      if (removes == 0)
         return 0;
      return TimeUnit.NANOSECONDS.toMillis(counters.get(StripeB.removeTimesFieldUpdater) / removes);
   }

   @ManagedAttribute(
         description = "Average number of nanoseconds for a remove operation in the cache",
         displayName = "Average remove time",
         units = Units.NANOSECONDS
   )
   public long getAverageRemoveTimeNanos() {
      long removes = getRemoveHits();
      if (removes == 0)
         return 0;
      return counters.get(StripeB.removeTimesFieldUpdater) / removes;
   }

   @ManagedAttribute(
         description = "Number of entries in the cache including passivated entries",
         displayName = "Number of current cache entries"
   )
   public int getNumberOfEntries() {
      return cache.wired().withFlags(Flag.CACHE_MODE_LOCAL).size();
   }

   @ManagedAttribute(
         description = "Number of entries currently in-memory excluding expired entries",
         displayName = "Number of in-memory cache entries"
   )
   public int getNumberOfEntriesInMemory() {
      return dataContainer.size();
   }

   @ManagedAttribute(
         description = "Amount of memory in bytes allocated for use in eviction for data in the cache",
         displayName = "Memory used by data in the cache"
   )
   public long getDataMemoryUsed() {
      if (cacheConfiguration.memory().isEvictionEnabled() && cacheConfiguration.memory().evictionType() == EvictionType.MEMORY) {
         return dataContainer.evictionSize();
      }
      return 0;
   }

   @ManagedAttribute(
         description = "Amount off-heap memory used by this cache (bytes)",
         displayName = "Off-Heap memory used"
   )
   public long getOffHeapMemoryUsed() {
      return allocator.getAllocatedAmount();
   }

   @ManagedAttribute(
         description = "Amount of nodes required to guarantee data consistency",
         displayName = "Required Minimum Nodes"
   )
   public int getRequiredMinimumNumberOfNodes() {
      return calculateRequiredMinimumNumberOfNodes(cache.wired(), componentRegistry);
   }

   public static int calculateRequiredMinimumNumberOfNodes(AdvancedCache<?, ?> cache, ComponentRegistry componentRegistry) {
      Configuration config = cache.getCacheConfiguration();

      ClusteringConfiguration clusteringConfiguration = config.clustering();
      CacheMode mode = clusteringConfiguration.cacheMode();
      if (mode.isReplicated() || !mode.isClustered()) {
         // Local and replicated only require the 1 node to keep the data
         return 1;
      }
      CacheTopology cacheTopology = cache.getDistributionManager().getCacheTopology();
      if (mode.isInvalidation()) {
         // Invalidation requires all as we don't know what data is installed on which
         return cacheTopology.getMembers().size();
      }
      int numMembers = cacheTopology.getMembers().size();
      // If scattered just assume 2 owners - numOwners in config says 1 though
      int numOwners = mode.isScattered() ? 2 : clusteringConfiguration.hash().numOwners();
      int minNodes = numMembers - numOwners + 1;
      long maxSize = config.memory().size();

      int evictionRestrictedNodes;
      if (maxSize > 0) {
         EvictionStrategy evictionStrategy = config.memory().evictionStrategy();
         long totalData;
         long capacity;
         switch (evictionStrategy) {
            case REMOVE:
               DataContainer dataContainer = cache.getDataContainer();
               totalData = dataContainer.evictionSize() * numOwners;
               capacity = dataContainer.capacity();
               break;
            case EXCEPTION:
               TransactionalExceptionEvictionInterceptor exceptionInterceptor = componentRegistry.getComponent(
                     TransactionalExceptionEvictionInterceptor.class);
               totalData = exceptionInterceptor.getCurrentSize();
               capacity = exceptionInterceptor.getMaxSize();
               break;
            default:
               throw new IllegalArgumentException("We only support remove or exception based strategy here");
         }
         evictionRestrictedNodes = (int) (totalData / capacity) + (totalData % capacity != 0 ? 1 : 0);
      } else {
         evictionRestrictedNodes = 1;
      }
      return Math.max(evictionRestrictedNodes, minNodes);
   }

   @ManagedAttribute(
         description = "Number of seconds since cache started",
         displayName = "Seconds since cache started",
         units = Units.SECONDS,
         measurementType = MeasurementType.TRENDSUP
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
         measurementType = MeasurementType.TRENDSUP
   )
   @Deprecated
   public long getElapsedTime() {
      // backward compatibility as we renamed ElapsedTime to TimeSinceStart
      return getTimeSinceStart();
   }

   @ManagedAttribute(
         description = "Number of seconds since the cache statistics were last reset",
         displayName = "Seconds since cache statistics were reset",
         units = Units.SECONDS
   )
   public long getTimeSinceReset() {
      return timeService.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
   }

   @Override
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

   private static class StripeA {
      @SuppressWarnings("unused")
      private long slack1, slack2, slack3, slack4, slack5, slack6, slack7, slack8;
   }

   @SuppressWarnings("VolatileLongOrDoubleField")
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

   private static final class StripeC extends StripeB {
      @SuppressWarnings("unused")
      private long slack1, slack2, slack3, slack4, slack5, slack6, slack7, slack8;
   }
}
