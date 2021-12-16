package org.infinispan.container.versioning.irac;

import static org.infinispan.remoting.transport.impl.VoidResponseCollector.ignoreLeavers;
import static org.infinispan.remoting.transport.impl.VoidResponseCollector.validOnly;
import static org.infinispan.util.concurrent.CompletableFutures.completedNull;
import static org.infinispan.util.concurrent.CompletableFutures.toTrueFunction;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.irac.IracTombstoneCleanupCommand;
import org.infinispan.commands.irac.IracTombstonePrimaryCheckCommand;
import org.infinispan.commands.irac.IracTombstoneRemoteSiteCheckCommand;
import org.infinispan.commands.irac.IracTombstoneStateResponseCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
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
import org.infinispan.xsite.irac.IracXSiteBackup;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableObserver;
import io.reactivex.rxjava3.core.CompletableSource;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Predicate;
import net.jcip.annotations.GuardedBy;

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

   @Inject DistributionManager distributionManager;
   @Inject RpcManager rpcManager;
   @Inject CommandsFactory commandsFactory;
   @Inject TakeOfflineManager takeOfflineManager;
   @Inject ComponentRef<IracManager> iracManager;
   @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   @Inject ScheduledExecutorService scheduledExecutorService;
   @Inject @ComponentName(KnownComponentNames.BLOCKING_EXECUTOR) Executor blockingExecutor;
   private final Map<Object, IracTombstoneInfo> tombstoneMap;
   private final IracExecutor iracExecutor;
   private final Collection<IracXSiteBackup> asyncBackups;
   private final Scheduler scheduler;
   private volatile boolean stopped = true;
   private final int batchSize;

   public DefaultIracTombstoneManager(Configuration configuration) {
      iracExecutor = new IracExecutor(this::performCleanup);
      asyncBackups = DefaultIracManager.asyncBackups(configuration);
      tombstoneMap = new ConcurrentHashMap<>(configuration.sites().tombstoneMapSize());
      scheduler = new Scheduler(configuration.sites().tombstoneMapSize(), configuration.sites().maxTombstoneCleanupDelay());
      batchSize = configuration.sites().asyncBackupsStream()
            .map(BackupConfiguration::stateTransfer)
            .map(XSiteStateTransferConfiguration::chunkSize)
            .reduce(1, Integer::max);

   }

   @Start
   public void start() {
      Transport transport = rpcManager.getTransport();
      transport.checkCrossSiteAvailable();
      String localSiteName = transport.localSiteName();
      asyncBackups.removeIf(xSiteBackup -> localSiteName.equals(xSiteBackup.getSiteName()));
      iracExecutor.setBackOff(ExponentialBackOff.NO_OP);
      iracExecutor.setExecutor(blockingExecutor);
      stopped = false;
      scheduler.disabled = false;
      scheduler.scheduleWithCurrentDelay();
   }

   @Stop
   public void stop() {
      stopped = true;
      stopCleanupTask();
      // drop everything
      tombstoneMap.clear();
   }

   // for testing purposes only!
   public void stopCleanupTask() {
      scheduler.disable();
   }

   public void storeTombstone(int segment, Object key, IracMetadata metadata) {
      tombstoneMap.put(key, new IracTombstoneInfo(key, segment, metadata));
   }

   @Override
   public void storeTombstoneIfAbsent(IracTombstoneInfo tombstone) {
      if (tombstone == null) {
         return;
      }
      tombstoneMap.putIfAbsent(tombstone.getKey(), tombstone);
   }

   @Override
   public IracMetadata getTombstone(Object key) {
      IracTombstoneInfo tombstone = tombstoneMap.get(key);
      return tombstone == null ? null : tombstone.getMetadata();
   }

   @Override
   public void removeTombstone(IracTombstoneInfo tombstone) {
      if (tombstone == null) {
         return;
      }
      tombstoneMap.remove(tombstone.getKey(), tombstone);
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

   @Override
   public void sendStateTo(Address requestor, IntSet segments) {
      StateTransferHelper helper = new StateTransferHelper(requestor, segments);
      Flowable.fromIterable(tombstoneMap.values())
            .filter(helper)
            .buffer(batchSize)
            .concatMapCompletableDelayError(helper)
            .subscribe(helper);
   }

   @Override
   public void checkStaleTombstone(Collection<? extends IracTombstoneInfo> tombstones) {
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      for (IracTombstoneInfo tombstone : tombstones) {
         IracTombstoneInfo data = tombstoneMap.get(tombstone.getKey());
         if (!topology.getSegmentDistribution(tombstone.getSegment()).isPrimary() || (tombstone.equals(data))) {
            // not a primary owner or the data is the same (i.e. it is valid)
            continue;
         }
         IracTombstoneCleanupCommand cmd = commandsFactory.buildIracTombstoneCleanupCommand(tombstone);
         rpcManager.sendToMany(topology.getSegmentDistribution(tombstone.getSegment()).writeOwners(), cmd, DeliverOrder.NONE);
      }
   }

   // Testing purposes
   public void startCleanupTombstone() {
      iracExecutor.run();
   }

   // Testing purposes
   public void runCleanupAndWait() {
      performCleanup().toCompletableFuture().join();
   }

   // Testing purposes
   public boolean contains(IracTombstoneInfo tombstone) {
      return tombstone.equals(tombstoneMap.get(tombstone.getKey()));
   }

   private CompletionStage<Void> performCleanup() {
      if (stopped) {
         return CompletableFutures.completedNull();
      }
      scheduler.onTaskStarted(tombstoneMap.size());
      try {
         AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
         RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
         Map<Address, BackPressure<IracTombstonePrimaryCheckCommand, Void>> staleTombstones = new HashMap<>(tombstoneMap.size());
         for (IracTombstoneInfo tombstone : tombstoneMap.values()) {
            DistributionInfo info = distributionManager.getCacheTopology().getSegmentDistribution(tombstone.getSegment());
            if (!info.isWriteOwner()) {
               // topology changed, no longer an owner
               removeTombstone(tombstone);
               continue;
            }
            // if we are a backup owner and the key is present in IracManager, skip checking for the stale tombstone;
            // it means the primary owner hasn't sent the update to the remote site.
            if (iracManager.running().containsKey(tombstone.getKey())) {
               continue;
            }
            if (!info.isPrimary()) {
               BackPressure<IracTombstonePrimaryCheckCommand, Void> backPressure = staleTombstones.get(info.primary());
               if (backPressure == null) {
                  backPressure = new BackPressure<>(commandsFactory.buildIracTombstonePrimaryCheckCommand(batchSize), null);
                  staleTombstones.put(info.primary(), backPressure);
               }
               if (backPressure.element.addTombstone(tombstone) == batchSize) {
                  CompletionStage<Void> rsp;
                  if (backPressure.delay == null) {
                     rsp = rpcManager.invokeCommand(info.primary(), backPressure.element, ignoreLeavers(), rpcOptions);
                  } else {
                     ReplicableCommand rCmd = backPressure.element;
                     rsp = backPressure.delay.thenComposeAsync(unused -> rpcManager.invokeCommand(info.primary(), rCmd, ignoreLeavers(), rpcOptions), blockingExecutor);
                  }
                  staleTombstones.put(info.primary(), new BackPressure<>(commandsFactory.buildIracTombstonePrimaryCheckCommand(batchSize), rsp));
               }
               continue;
            }
            stage.dependsOn(new CleanupTask(tombstone).checkRemoteSites());
         }

         //sending any pending stale tombstones
         for (Map.Entry<Address, BackPressure<IracTombstonePrimaryCheckCommand, Void>> entry : staleTombstones.entrySet()) {
            BackPressure<IracTombstonePrimaryCheckCommand, Void> backPressure = entry.getValue();
            if (backPressure.element.isEmpty()) {
               continue;
            }
            if (backPressure.delay == null) {
               rpcManager.sendTo(entry.getKey(), backPressure.element, DeliverOrder.NONE);
            } else {
               backPressure.delay.thenRunAsync(() -> rpcManager.sendTo(entry.getKey(), backPressure.element, DeliverOrder.NONE), blockingExecutor);
            }

         }
         return stage.freeze().whenComplete(scheduler);
      } catch (Throwable t) {
         log.debug("Unexpected exception", t);
         scheduler.scheduleWithCurrentDelay();
         return CompletableFutures.completedNull();
      }
   }

   private DistributionInfo getSegmentDistribution(int segment) {
      return distributionManager.getCacheTopology().getSegmentDistribution(segment);
   }

   private final class CleanupTask implements Function<Boolean, CompletionStage<Void>>, Runnable {
      private final IracTombstoneInfo tombstone;

      private CleanupTask(IracTombstoneInfo tombstone) {
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
            IracTombstoneRemoteSiteCheckCommand cmd = commandsFactory.buildIracTombstoneRemoteSiteCheckCommand(tombstone.getKey());
            stage.dependsOn(rpcManager.invokeXSite(backup, cmd));
         }
         // in case of exception, keep the tombstone
         return stage.freeze()
               .exceptionally(toTrueFunction())
               .thenComposeAsync(this, blockingExecutor);
      }

      @Override
      public CompletionStage<Void> apply(Boolean keepTombstone) {
         if (keepTombstone) {
            return completedNull();
         }
         // send commit to all write owner
         IracTombstoneCleanupCommand cmd = commandsFactory.buildIracTombstoneCleanupCommand(tombstone);
         return rpcManager.invokeCommand(getSegmentDistribution(tombstone.getSegment()).writeOwners(),
               cmd, validOnly(), rpcManager.getSyncRpcOptions()).thenRunAsync(this, blockingExecutor);
      }

      @Override
      public void run() {
         removeTombstone(tombstone);
      }
   }

   private static final class BackPressure<E, T> {
      final E element;
      final CompletionStage<T> delay;

      private BackPressure(E element, CompletionStage<T> delay) {
         this.element = element;
         this.delay = delay;
      }
   }

   private class StateTransferHelper implements Predicate<IracTombstoneInfo>,
         io.reactivex.rxjava3.functions.Function<Collection<IracTombstoneInfo>, CompletableSource>,
         CompletableObserver {
      private final Address requestor;
      private final IntSet segments;

      private StateTransferHelper(Address requestor, IntSet segments) {
         this.requestor = requestor;
         this.segments = segments;
      }

      @Override
      public boolean test(IracTombstoneInfo tombstone) {
         return segments.contains(tombstone.getSegment());
      }

      @Override
      public CompletableSource apply(Collection<IracTombstoneInfo> state) {
         RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
         IracTombstoneStateResponseCommand cmd = commandsFactory.buildIracTombstoneStateResponseCommand(state);
         CompletionStage<Void> rsp = rpcManager.invokeCommand(requestor, cmd, ignoreLeavers(), rpcOptions);
         return Completable.fromCompletionStage(rsp);
      }

      @Override
      public void onSubscribe(@NonNull Disposable d) {
         //no-op
      }

      @Override
      public void onComplete() {
         if (log.isDebugEnabled()) {
            log.debugf("Tombstones transferred to %s for segments %s", requestor, segments);
         }
      }

      @Override
      public void onError(@NonNull Throwable e) {
         log.failedToTransferTombstones(requestor, segments, e);
      }
   }

   private final class Scheduler implements BiConsumer<Void, Throwable> {
      final int targetSize;
      final long maxDelayMillis;

      int preCleanupSize;
      int previousPostCleanupSize;

      long currentDelayMillis;
      volatile boolean running;
      volatile boolean disabled;
      @GuardedBy("this")
      ScheduledFuture<?> future;

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

      synchronized void scheduleWithCurrentDelay() {
         running = false;
         if (stopped || disabled) {
            return;
         }
         if (future != null) {
            future.cancel(true);
         }
         future = scheduledExecutorService.schedule(iracExecutor, currentDelayMillis, TimeUnit.MILLISECONDS);
      }

      synchronized void disable() {
         disabled = true;
         if (future != null) {
            future.cancel(true);
            future = null;
         }
      }

      @Override
      public void accept(Void unused, Throwable throwable) {
         // invoked after the cleanup round
         onTaskCompleted(tombstoneMap.size());
      }
   }
}
