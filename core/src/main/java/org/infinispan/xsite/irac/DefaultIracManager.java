package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracCleanupKeyCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ExponentialBackOff;
import org.infinispan.util.ExponentialBackOffImpl;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;

/**
 * Default implementation of {@link IracManager}.
 * <p>
 * It tracks the keys updated by this site and sends them, periodically, to the configured remote sites.
 * <p>
 * The primary owner coordinates everything. It sends the updates request to the remote site and coordinates the local
 * site backup owners. After sending the updates to the remote site, it sends a cleanup request to the local site backup
 * owners
 * <p>
 * The backup owners only keeps a backup list of the tracked keys.
 * <p>
 * On topology change, the updated keys list is replicate to the new owner(s). Also, if a segment is being transferred
 * (i.e. the primary owner isn't a write and read owner), no updates to the remote site is sent since, most likely, the
 * node doesn't have the most up-to-date value.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracManager implements IracManager, Runnable {

   private static final Log log = LogFactory.getLog(DefaultIracManager.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject RpcManager rpcManager;
   @Inject Configuration config;
   @Inject TakeOfflineManager takeOfflineManager;
   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject CommandsFactory commandsFactory;
   @Inject IracVersionGenerator iracVersionGenerator;

   private final Map<Object, Object> updatedKeys;
   private final Semaphore senderNotifier;
   private volatile ExponentialBackOff backOff;
   private volatile boolean hasClear;
   private volatile Collection<XSiteBackup> asyncBackups;
   private volatile Thread sender;
   private volatile boolean running;

   public DefaultIracManager() {
      this.updatedKeys = new ConcurrentHashMap<>();
      this.senderNotifier = new Semaphore(0);
      this.backOff = new ExponentialBackOffImpl();
   }

   private static Collection<XSiteBackup> asyncBackups(Configuration config, String localSiteName) {
      return config.sites().asyncBackupsStream()
            .filter(bc -> !localSiteName.equals(bc.site()))
            .map(bc -> new XSiteBackup(bc.site(), true, bc.replicationTimeout())) //convert to sync
            .collect(Collectors.toList());
   }

   private static Stream<?> keyStream(WriteCommand command) {
      return command.getAffectedKeys().stream();
   }

   private static boolean backupToRemoteSite(WriteCommand command) {
      return !command.hasAnyFlag(FlagBitSets.SKIP_XSITE_BACKUP);
   }

   private static IntSet newIntSet(Address ignored) {
      return IntSets.mutableEmptySet();
   }

   @Start
   public void start() {
      Transport transport = rpcManager.getTransport();
      transport.checkCrossSiteAvailable();
      String localSiteName = transport.localSiteName();
      asyncBackups = asyncBackups(config, localSiteName);
      if (trace) {
         String b = asyncBackups.stream().map(XSiteBackup::getSiteName).collect(Collectors.joining(", "));
         log.tracef("Async remote sites found: %s", b);
      }
      Thread oldSender = sender;
      if (oldSender != null) {
         oldSender.interrupt();
      }
      senderNotifier.drainPermits();
      running = true;
      hasClear = false;
      //TODO should we use a runnable in blocking executor?
      // it requires some synchronization since we can stop the runnable if the map is empty (instead of having a thread blocked/waiting)
      // not for now...
      Thread newSender = new Thread(this, "irac-sender-thread-" + transport.getAddress());
      sender = newSender;
      newSender.start();
   }

   @Stop
   public void stop() {
      running = false;
      Thread oldSender = sender;
      if (oldSender != null) {
         oldSender.interrupt();
      }
   }

   @Override
   public void trackUpdatedKey(Object key, Object lockOwner) {
      if (trace) {
         log.tracef("Tracking key for %s: %s", lockOwner, key);
      }
      updatedKeys.put(key, lockOwner);
      senderNotifier.release();
   }

   @Override
   public <K> void trackUpdatedKeys(Collection<K> keys, Object lockOwner) {
      if (trace) {
         log.tracef("Tracking keys for %s: %s", lockOwner, keys);
      }
      if (keys.isEmpty()) {
         return;
      }
      keys.forEach(key -> updatedKeys.put(key, lockOwner));
      senderNotifier.release();
   }

   @Override
   public void trackKeysFromTransaction(Stream<WriteCommand> modifications, GlobalTransaction lockOwner) {
      keysFromMods(modifications).forEach(key -> {
         if (trace) {
            log.tracef("Tracking key for %s: %s", lockOwner, key);
         }
         updatedKeys.put(key, lockOwner);
      });
      senderNotifier.release();
   }

   @Override
   public void trackClear() {
      if (trace) {
         log.trace("Tracking clear request");
      }
      hasClear = true;
      updatedKeys.clear();
      senderNotifier.release();
   }

   @Override
   public void cleanupKey(Object key, Object lockOwner, IracMetadata tombstone) {
      boolean removed = updatedKeys.remove(key, lockOwner);
      iracVersionGenerator.removeTombstone(key, tombstone);
      if (trace) {
         log.tracef("Removing key '%s'. LockOwner='%s', removed=%s", key, lockOwner, removed);
      }
   }

   @Override
   public void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology) {
      if (trace) {
         log.trace("[IRAC] Topology Updated. Checking pending keys.");
      }
      Address local = rpcManager.getAddress();
      if (!newCacheTopology.getMembers().contains(local)) {
         //not in teh cache topology
         return;
      }
      IntSet addedSegments = IntSets.mutableCopyFrom(newCacheTopology.getWriteConsistentHash().getSegmentsForOwner(local));
      if (oldCacheTopology.getMembers().contains(local)) {
         addedSegments.removeAll(oldCacheTopology.getWriteConsistentHash().getSegmentsForOwner(local));
      }

      if (addedSegments.isEmpty()) {
         //this node doesn't have any new segments but it may become the primary owner of some.
         //trigger a new round (it also removes the keys if the node isn't a owner)
         senderNotifier.release();
         return;
      }

      //group new segments by primary owner
      Map<Address, IntSet> primarySegments = new HashMap<>();
      for (int segment : addedSegments) {
         Address primary = newCacheTopology.getWriteConsistentHash().locatePrimaryOwnerForSegment(segment);
         primarySegments.computeIfAbsent(primary, DefaultIracManager::newIntSet).add(segment);
      }

      primarySegments.forEach(this::sendStateRequest);

      senderNotifier.release();
   }

   @Override
   public void requestState(Address origin, IntSet segments) {
      updatedKeys.forEach((key, lockOwner) -> sendStateIfNeeded(origin, segments, key, lockOwner));
   }

   @Override
   public void receiveState(Object key, Object lockOwner, IracMetadata tombstone) {
      iracVersionGenerator.storeTombstoneIfAbsent(key, tombstone);
      updatedKeys.putIfAbsent(key, lockOwner);
      senderNotifier.release();
   }

   //public for testing purposes
   public void sendStateIfNeeded(Address origin, IntSet segments, Object key, Object lockOwner) {
      int segment = getSegment(key);
      if (!segments.contains(segment)) {
         return;
      }
      //send the tombstone too
      IracMetadata tombstone = iracVersionGenerator.getTombstone(key);

      CacheRpcCommand cmd = commandsFactory.buildIracStateResponseCommand(key, lockOwner, tombstone);
      rpcManager.sendTo(origin, cmd, DeliverOrder.NONE);
   }

   //public for testing purposes
   public Stream<?> keysFromMods(Stream<WriteCommand> modifications) {
      return modifications
            .filter(WriteCommand::isSuccessful)
            .filter(DefaultIracManager::backupToRemoteSite)
            .flatMap(DefaultIracManager::keyStream)
            .filter(this::isWriteOwner);
   }

   @Override
   public void run() {
      try {
         while (running) {
            senderNotifier.acquire();
            senderNotifier.drainPermits();
            periodicSend();
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }

   public void setBackOff(ExponentialBackOff backOff) {
      this.backOff = Objects.requireNonNull(backOff);
   }

   public boolean isEmpty() {
      return updatedKeys.isEmpty();
   }

   private void sendStateRequest(Address primary, IntSet segments) {
      CacheRpcCommand cmd = commandsFactory.buildIracRequestStateCommand(segments);
      rpcManager.sendTo(primary, cmd, DeliverOrder.NONE);
   }


   private ResponseResult awaitResponses(CompletionStage<Void> reply) throws InterruptedException {
      //wait for replies
      try {
         reply.toCompletableFuture().get();
         return ResponseResult.OK;
      } catch (ExecutionException e) {
         //can be ignored. it will be retried in the next round.
         if (trace) {
            log.trace("IRAC update not successful.", e);
         }
         //if it fails, we release a permit so the thread can retry
         //otherwise, if the cluster is idle, the keys will never been sent to the remote site
         senderNotifier.release();
         return DefaultTakeOfflineManager.isCommunicationError(e) ?
               ResponseResult.NETWORK_EXCEPTION :
               ResponseResult.REMOTE_EXCEPTION;
      }
   }

   private void periodicSend() throws InterruptedException {
      if (trace) {
         log.tracef("[IRAC] Sending keys to remote site(s). Has clear? %s, keys: %s", hasClear, updatedKeys.keySet());
      }
      if (hasClear) {
         //make sure the clear is replicated everywhere before sending the updates!
         CompletionStage<Void> rsp = sendCommandToAllBackups(commandsFactory.buildIracClearKeysCommand());
         switch (awaitResponses(rsp)) {
            case REMOTE_EXCEPTION:
               //an exception occurred. we need to retry
               backOff.reset();
               return;
            case NETWORK_EXCEPTION:
               //network exception. backoff to avoid overloading the receiving site
               backOff.backoffSleep();
               return;
            case OK:
               //everything cleared from remote site. continue with the new updates
               hasClear = false;
               backOff.reset();
               break;
         }
      }
      try {
         SendKeyTask task = new SendKeyTask();
         updatedKeys.forEach(task);
         task.await();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw e;
      } catch (Throwable t) {
         //it should never happen. SendKeyTask must handle all exceptions!
         log.unexpectedErrorFromIrac(t);
      }
   }

   private XSiteResponse sendToRemoteSite(XSiteBackup backup, XSiteReplicateCommand cmd) {
      XSiteResponse rsp = rpcManager.invokeXSite(backup, cmd);
      takeOfflineManager.registerRequest(rsp);
      return rsp;
   }

   private void removeKey(Object key, int segmentId, Object lockOwner, IracMetadata tombstone) {
      if (trace) {
         log.tracef("Replication completed for key '%s'. Lock Owner='%s'", key, lockOwner);
      }
      //removes the key from the "updated keys map" in all owners.
      DistributionInfo dInfo = getDistributionInfo(segmentId);
      IracCleanupKeyCommand cmd = commandsFactory.buildIracCleanupKeyCommand(key, lockOwner, tombstone);
      rpcManager.sendToMany(dInfo.writeOwners(), cmd, DeliverOrder.NONE);
      cleanupKey(key, lockOwner, tombstone);
   }

   private DistributionInfo getDistributionInfoKey(Object key) {
      return clusteringDependentLogic.getCacheTopology().getDistribution(key);
   }

   private DistributionInfo getDistributionInfo(int segmentId) {
      return clusteringDependentLogic.getCacheTopology().getDistribution(segmentId);
   }

   private int getSegment(Object key) {
      return clusteringDependentLogic.getCacheTopology().getSegment(key);
   }

   private boolean isWriteOwner(Object key) {
      return getDistributionInfoKey(key).isWriteOwner();
   }

   private CompletionStage<Void> sendCommandToAllBackups(XSiteReplicateCommand command) {
      if (command == null) {
         return CompletableFutures.completedNull();
      }
      AggregateCompletionStage<Void> collector = CompletionStages.aggregateCompletionStage();
      for (XSiteBackup backup : asyncBackups) {
         if (takeOfflineManager.getSiteState(backup.getSiteName()) == SiteState.OFFLINE) {
            continue; //backup is offline
         }
         collector.dependsOn(sendToRemoteSite(backup, command));
      }
      return collector.freeze();
   }

   private XSiteReplicateCommand buildRemoveCommand(CleanupTask cleanupTask) {
      Object key = cleanupTask.key;
      IracMetadata metadata = iracVersionGenerator.getTombstone(key);
      if (metadata == null) {
         return null;
      }
      cleanupTask.tombstone = metadata;
      return commandsFactory.buildIracRemoveKeyCommand(key, metadata);
   }

   private CompletionStage<InternalCacheEntry<Object, Object>> fetchEntry(Object key, int segmentId) {
      return clusteringDependentLogic.getEntryLoader().loadAndStoreInDataContainer(key, segmentId);
   }

   /**
    * A round of updates to send to the remote site.
    */
   private class SendKeyTask implements BiConsumer<Object, Object> {

      private final List<CompletionStage<Void>> responses;
      private final List<CleanupTask> cleanupTasks;

      private SendKeyTask() {
         responses = new LinkedList<>();
         cleanupTasks = new LinkedList<>();
      }

      @Override
      public void accept(Object key, Object lockOwner) {
         DistributionInfo dInfo = getDistributionInfoKey(key);
         if (!dInfo.isPrimary()) {
            return; //backup owner, nothing to send
         } else if (!dInfo.isWriteOwner()) {
            //topology changed! cleanup the key
            cleanupTasks.add(new CleanupTask(key, dInfo.segmentId(), lockOwner));
            return;
         } else if (!dInfo.isReadOwner()) {
            //state transfer in progress (we are a write owner but not a read owner)
            //we need the data in DataContainer and CacheLoaders, so we must wait until we receive the key or will end up sending a remove update.
            //when the new topology arrives, this will be triggered again
            return;
         }

         CleanupTask cleanupTask = new CleanupTask(key, dInfo.segmentId(), lockOwner);

         CompletionStage<Void> rsp = fetchEntry(key, dInfo.segmentId())
               .thenApply(lEntry -> lEntry == null ?
                     buildRemoveCommand(cleanupTask) :
                     commandsFactory.buildIracPutKeyCommand(lEntry))
               .thenCompose(DefaultIracManager.this::sendCommandToAllBackups)
               .thenRun(cleanupTask);
         responses.add(rsp);
      }

      void await() throws InterruptedException {
         //cleanup everything not needed
         cleanupTasks.forEach(CleanupTask::run);

         boolean needsBackoff = false;
         //wait for replies
         for (CompletionStage<Void> rsp : responses) {
            if (awaitResponses(rsp) == ResponseResult.NETWORK_EXCEPTION) {
               needsBackoff = true;
            }
         }
         if (needsBackoff) {
            backOff.backoffSleep();
         } else {
            backOff.reset();
         }
      }
   }

   /**
    * Cleanup task run after a successful replication to the remote site.
    */
   private class CleanupTask implements Runnable {

      final Object key;
      final int segmentId;
      final Object lockOwner;
      volatile IracMetadata tombstone;

      private CleanupTask(Object key, int segmentId, Object lockOwner) {
         this.key = key;
         this.segmentId = segmentId;
         this.lockOwner = lockOwner;
      }

      @Override
      public void run() {
         removeKey(key, segmentId, lockOwner, tombstone);
      }
   }

   private enum ResponseResult {
      OK,
      REMOTE_EXCEPTION,
      NETWORK_EXCEPTION
   }
}
