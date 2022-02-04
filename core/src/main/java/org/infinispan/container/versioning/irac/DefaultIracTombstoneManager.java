package org.infinispan.container.versioning.irac;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracCleanupTombstoneCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.util.ExponentialBackOff;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.irac.DefaultIracManager;
import org.infinispan.xsite.irac.IracExecutor;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;

/**
 * A default implementation for {@link IracTombstoneManager}.
 * <p>
 * This class is responsible to keep track of the tombstones for the IRAC algorithm. Tombstones are used when a key is
 * removed but its metadata is necessary to detect possible conflicts in this and remote sites. When all sites have
 * updated the key, the tombstone can be removed.
 * <p>
 * Tombstones are removed periodically in the background.
 *
 * @since 14.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracTombstoneManager implements IracTombstoneManager {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject
   DistributionManager distributionManager;
   @Inject
   RpcManager rpcManager;
   @Inject
   CommandsFactory commandsFactory;
   @Inject
   TakeOfflineManager takeOfflineManager;
   @Inject
   ComponentRef<IracManager> iracManager;
   @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   @Inject ScheduledExecutorService scheduledExecutorService;
   private final Map<Object, TombstoneData> tombstoneMap;
   private final IracExecutor iracExecutor;
   private final Collection<XSiteBackup> asyncBackups;
   private final Scheduler scheduler;
   private volatile boolean stopped = true;

   public DefaultIracTombstoneManager(Configuration configuration) {
      this.iracExecutor = new IracExecutor(this::performCleanup);
      this.asyncBackups = DefaultIracManager.asyncBackups(configuration);
      this.tombstoneMap = new ConcurrentHashMap<>(configuration.sites().tombstoneMapSize());
      scheduler = new Scheduler(configuration.sites().tombstoneMapSize(), configuration.sites().maxTombstoneCleanupDelay());
   }

   @Inject
   public void inject(@ComponentName(KnownComponentNames.BLOCKING_EXECUTOR) Executor blockingExecutor) {
      // using the inject method here in order to decrease the class size
      iracExecutor.setBackOff(ExponentialBackOff.NO_OP);
      iracExecutor.setExecutor(blockingExecutor);
   }

   @Start
   public void start() {
      Transport transport = rpcManager.getTransport();
      transport.checkCrossSiteAvailable();
      String localSiteName = transport.localSiteName();
      asyncBackups.removeIf(xSiteBackup -> localSiteName.equals(xSiteBackup.getSiteName()));
      stopped = false;
      scheduler.scheduleWithCurrentDelay();
   }

   @Stop
   public void stop() {
      stopped = true;
   }

   public void storeTombstone(int segment, Object key, IracMetadata metadata) {
      tombstoneMap.put(key, new TombstoneData(segment, metadata));
   }

   @Override
   public void storeTombstoneIfAbsent(int segment, Object key, IracMetadata metadata) {
      if (metadata == null) {
         return;
      }
      tombstoneMap.putIfAbsent(key, new TombstoneData(segment, metadata));
   }

   @Override
   public IracMetadata getTombstone(Object key) {
      TombstoneData data = tombstoneMap.get(key);
      return data == null ? null : data.getMetadata();
   }

   @Override
   public void removeTombstone(Object key, IracMetadata iracMetadata) {
      if (iracMetadata == null) {
         return;
      }
      remove(key, new TombstoneData(-1, iracMetadata));
   }

   @Override
   public void removeTombstone(Object key) {
      tombstoneMap.remove(key);
   }


   @Override
   public boolean isEmpty() {
      return tombstoneMap.isEmpty();
   }

   @Override
   public int size() {
      return tombstoneMap.size();
   }

   @Override
   public boolean isTaskRunning() {
      return scheduler.running;
   }

   @Override
   public long getCurrentDelayMillis() {
      return scheduler.currentDelayMillis;
   }

   public void startCleanupTombstone() {
      iracExecutor.run();
   }

   private CompletionStage<Void> performCleanup() {
      if (stopped) {
         return CompletableFutures.completedNull();
      }
      scheduler.onTaskStarted(tombstoneMap.size());
      try {
         AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
         for (Map.Entry<Object, TombstoneData> entry : tombstoneMap.entrySet()) {
            DistributionInfo info = distributionManager.getCacheTopology().getSegmentDistribution(entry.getValue().getSegment());
            if (!info.isWriteOwner()) {
               // topology changed, no longer an owner
               remove(entry.getKey(), entry.getValue());
               continue;
            }
            if (!info.isPrimary() || iracManager.running().containsKey(entry.getKey())) {
               // backup owner or the irac manager haven't sent the update successfully
               continue;
            }
            stage.dependsOn(new CleanupTask(entry.getKey(), entry.getValue()).checkRemoteSites());
         }

         return stage.freeze().whenComplete(scheduler);
      } catch (Throwable t) {
         log.debug("Unexpected exception", t);
         scheduler.scheduleWithCurrentDelay();
         return CompletableFutures.completedNull();
      }
   }

   private void remove(Object key, TombstoneData data) {
      tombstoneMap.remove(key, data);
   }

   private DistributionInfo getSegmentDistribution(int segment) {
      return distributionManager.getCacheTopology().getSegmentDistribution(segment);
   }

   private static class TombstoneData {
      private final int segment;
      private final IracMetadata metadata;

      private TombstoneData(int segment, IracMetadata metadata) {
         this.segment = segment;
         this.metadata = Objects.requireNonNull(metadata);
      }

      public int getSegment() {
         return segment;
      }

      public IracMetadata getMetadata() {
         return metadata;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         TombstoneData that = (TombstoneData) o;
         return metadata.equals(that.metadata);
      }

      @Override
      public int hashCode() {
         return metadata.hashCode();
      }
   }

   private class CleanupTask implements Function<Boolean, CompletionStage<Void>>, Runnable {
      private final Object key;
      private final TombstoneData tombstone;

      private CleanupTask(Object key, TombstoneData tombstone) {
         this.key = key;
         this.tombstone = tombstone;
      }

      CompletionStage<Void> checkRemoteSites() {
         // if one of the site return true (i.e. the key is in updateKeys map, then do not remove it)
         AggregateCompletionStage<Boolean> stage = CompletionStages.orBooleanAggregateCompletionStage();
         for (XSiteBackup backup : asyncBackups) {
            if (takeOfflineManager.getSiteState(backup.getSiteName()) == SiteState.OFFLINE) {
               continue; // backup is offline
            }
            // we don't need the tombstone to query the remote site
            IracCleanupTombstoneCommand cmd = commandsFactory.buildIracCleanupTombstoneCommand(key, null);
            stage.dependsOn(rpcManager.invokeXSite(backup, cmd));
         }
         // in case of exception, keep the tombstone
         return stage.freeze()
               .exceptionally(CompletableFutures.toTrueFunction())
               .thenCompose(this);
      }

      @Override
      public CompletionStage<Void> apply(Boolean keepTombstone) {
         if (keepTombstone) {
            return CompletableFutures.completedNull();
         }
         // send commit to all write owner
         IracCleanupTombstoneCommand cmd = commandsFactory.buildIracCleanupTombstoneCommand(key, tombstone.getMetadata());
         return rpcManager.invokeCommand(getSegmentDistribution(tombstone.getSegment()).writeOwners(),
               cmd, VoidResponseCollector.validOnly(), rpcManager.getSyncRpcOptions()).thenRun(this);
      }

      @Override
      public void run() {
         remove(key, tombstone);
      }
   }

   private final class Scheduler implements BiConsumer<Void, Throwable> {
      final int targetSize;
      final long maxDelayMillis;

      int preCleanupSize;
      int previousPostCleanupSize;

      long currentDelayMillis;
      volatile boolean running;

      private Scheduler(int targetSize, long maxDelayMillis) {
         this.targetSize = targetSize;
         this.maxDelayMillis = maxDelayMillis;
         currentDelayMillis = maxDelayMillis / 2;
      }

      void onTaskStarted(int size) {
         running = true;
         preCleanupSize = size;
      }

      void onTaskCompleted(int postCleanupSize) {
         if (postCleanupSize >= targetSize) {
            // The tombstones map is already at or above the target size, start a new cleanup round immediately
            // Keep the delay >= 1 to simplify the tombstoneCreationRate calculation
            currentDelayMillis = 1;
         } else {
            // Estimate how long it would take for the tombstones map to reach the target size
            double tombstoneCreationRate = (preCleanupSize - previousPostCleanupSize) * 1.0 / currentDelayMillis;
            double estimationMillis;
            if (tombstoneCreationRate <= 0) {
               // The tombstone map will never reach the target size, use the maximum delay
               estimationMillis = maxDelayMillis;
            } else {
               // Ensure that 1 <= estimation <= maxDelayMillis
               estimationMillis = Math.min((targetSize - postCleanupSize) / tombstoneCreationRate + 1, maxDelayMillis);
            }
            // Use a geometric average between the current estimation and the previous one
            // to dampen the changes as the rate changes from one interval to the next
            // (especially when the interval duration is very short)
            currentDelayMillis = Math.round(Math.sqrt(currentDelayMillis * estimationMillis));
         }
         previousPostCleanupSize = postCleanupSize;
         scheduleWithCurrentDelay();
      }

      void scheduleWithCurrentDelay() {
         running = false;
         if (stopped) {
            return;
         }
         scheduledExecutorService.schedule(iracExecutor, currentDelayMillis, TimeUnit.MILLISECONDS);
      }

      @Override
      public void accept(Void unused, Throwable throwable) {
         // invoked after the cleanup round
         onTaskCompleted(tombstoneMap.size());
      }
   }
}
