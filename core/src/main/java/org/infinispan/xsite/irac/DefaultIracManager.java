package org.infinispan.xsite.irac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracCleanupKeyCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.status.TakeOfflineManager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracManager implements IracManager, Runnable {

   private static final Log log = LogFactory.getLog(DefaultIracManager.class);
   private static final boolean trace = log.isTraceEnabled();

   private final String localSiteName;
   private final Map<Object, Object> updatedKeys;
   private final Semaphore senderNotifier;
   @Inject
   RpcManager rpcManager;
   @Inject
   Transport transport; //TODO! remove when we drop support for local caches
   @Inject
   Configuration config;
   @Inject
   TakeOfflineManager takeOfflineManager;
   @Inject
   ClusteringDependentLogic clusteringDependentLogic;
   @Inject
   InternalDataContainer<Object, Object> dataContainer;
   @Inject
   PersistenceManager persistenceManager;
   @Inject
   CommandsFactory commandsFactory;
   private volatile boolean hasClear;
   private volatile Collection<XSiteBackup> asyncBackups;
   private volatile Thread sender;
   private volatile boolean running;

   public DefaultIracManager(String localSiteName) {
      this.localSiteName = localSiteName;
      this.updatedKeys = new ConcurrentHashMap<>();
      this.senderNotifier = new Semaphore(0);
   }

   private static Collection<XSiteBackup> asyncBackups(Collection<BackupConfiguration> config, String localSiteName) {
      List<XSiteBackup> res = new ArrayList<>(4);
      for (BackupConfiguration bc : config) {
         if (bc.site().equals(localSiteName)) {
            log.cacheBackupsDataToSameSite(localSiteName);
            continue;
         }
         if (bc.isAsyncBackup()) {
            //convert to sync to send a synchronous request
            res.add(new XSiteBackup(bc.site(), true, bc.replicationTimeout()));
         }
      }
      return res.isEmpty() ? Collections.emptyList() : res;
   }

   @Start
   public void start() {
      asyncBackups = asyncBackups(config.sites().enabledBackups(), localSiteName);
      if (trace) {
         Collection<String> b = asyncBackups.stream().map(XSiteBackup::getSiteName).collect(Collectors.toList());
         log.tracef("Async remote sites found: %s", b);
      }
      Thread oldSender = sender;
      if (oldSender != null) {
         oldSender.interrupt();
      }
      senderNotifier.drainPermits();
      running = true;
      hasClear = false;
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
   public void trackClear() {
      if (trace) {
         log.trace("Tracking clear request");
      }
      hasClear = true;
      updatedKeys.clear();
      senderNotifier.release();
   }

   @Override
   public void cleanupKey(Object key, Object lockOwner) {
      updatedKeys.remove(key, lockOwner);
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

   private boolean awaitResponses(CompletionStage<Void> reply) throws InterruptedException {
      //wait for replies
      try {
         reply.toCompletableFuture().get();
         return true;
      } catch (ExecutionException e) {
         log.trace("IRAC update failed!", e);
         //if it fails, we release a permit so the thread can retry
         //otherwise, if the cluster is idle, the keys will never been sent to the remote site
         senderNotifier.release();
      }
      return false;
   }

   private void periodicSend() throws InterruptedException {
      if (trace) {
         log.tracef("IRAC sending keys to remote site(s). Has clear? %s, keys: %s", hasClear, updatedKeys.keySet());
      }
      if (hasClear) {
         //make sure the clear is replicated everywhere before sending the updates!
         CompletionStage<Void> rsp = sendCommandToAllBackups(buildClearCommand());
         if (awaitResponses(rsp)) {
            hasClear = false;
         } else {
            //we got an exception.
            return;
         }
      }
      SendKeyTask task = new SendKeyTask();
      updatedKeys.forEach(task);
      task.await();
   }

   private XSiteResponse sendToRemoteSite(XSiteBackup backup, XSiteReplicateCommand cmd) {
      XSiteResponse rsp;
      if (rpcManager == null) {
         rsp = transport.backupRemotely(backup, cmd);
      } else {
         rsp = rpcManager.invokeXSite(backup, cmd);
      }
      takeOfflineManager.registerRequest(rsp);
      return rsp;
   }

   private void removeKey(Object key, int segmentId, Object lockOwner) {
      updatedKeys.remove(key, lockOwner);

      if (rpcManager != null) {
         DistributionInfo dInfo = getDistributionInfo(segmentId);
         IracCleanupKeyCommand cmd = commandsFactory.buildIracCleanupKeyCommand(key, lockOwner);
         rpcManager.sendToMany(dInfo.writeOwners(), cmd, DeliverOrder.NONE);
      }
   }

   private DistributionInfo getDistributionInfoKey(Object key) {
      return clusteringDependentLogic.getCacheTopology().getDistribution(key);
   }

   private DistributionInfo getDistributionInfo(int segmentId) {
      return clusteringDependentLogic.getCacheTopology().getSegmentDistribution(segmentId);
   }

   private CompletionStage<Void> sendCommandToAllBackups(XSiteReplicateCommand command) {
      AggregateCompletionStage<Void> collector = CompletionStages.aggregateCompletionStage();
      for (XSiteBackup backup : asyncBackups) {
         if (takeOfflineManager.isOffline(backup.getSiteName())) {
            continue; //backup is offline
         }
         collector.dependsOn(sendToRemoteSite(backup, command));
      }
      return collector.freeze();
   }

   private XSiteReplicateCommand buildClearCommand() {
      return commandsFactory.buildIracUpdateKeyCommand(null, null, null);
   }

   private InternalCacheEntry<Object, Object> fetchEntry(Object key, int segmentId) {
      LoadEntry entry = new LoadEntry(segmentId);
      dataContainer.compute(segmentId, key, entry);
      return entry.loadedEntry;
   }


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
         }

         InternalCacheEntry<Object, Object> entry = fetchEntry(key, dInfo.segmentId());
         XSiteReplicateCommand cmd = entry == null ?
                                     commandsFactory.buildIracUpdateKeyCommand(key, null, null) :
                                     commandsFactory.buildIracUpdateKeyCommand(entry);
         CleanupTask task = new CleanupTask(key, dInfo.segmentId(), lockOwner);
         CompletionStage<Void> rsp = sendCommandToAllBackups(cmd);
         rsp.thenRun(task);
         responses.add(rsp);
      }

      void await() throws InterruptedException {
         //cleanup everything not needed
         cleanupTasks.forEach(CleanupTask::run);

         //wait for replies
         for (CompletionStage<Void> rsp : responses) {
            awaitResponses(rsp);
         }
      }
   }

   private class CleanupTask implements Runnable {

      final Object key;
      final int segmentId;
      final Object lockOwner;

      private CleanupTask(Object key, int segmentId, Object lockOwner) {
         this.key = key;
         this.segmentId = segmentId;
         this.lockOwner = lockOwner;
      }

      @Override
      public void run() {
         removeKey(key, segmentId, lockOwner);
      }
   }

   private class LoadEntry implements DataContainer.ComputeAction<Object, Object> {

      private final int segmentId;
      private volatile InternalCacheEntry<Object, Object> loadedEntry;

      private LoadEntry(int segmentId) {
         this.segmentId = segmentId;
      }

      @Override
      public InternalCacheEntry<Object, Object> compute(Object key, InternalCacheEntry<Object, Object> oldEntry,
            InternalEntryFactory factory) {
         if (oldEntry != null) {
            loadedEntry = oldEntry;
            return oldEntry;
         }
         MarshallableEntry<Object, Object> mEntry = persistenceManager
               .loadFromAllStoresSync(key, segmentId, true, true);
         if (mEntry == null) {
            return null;
         }
         loadedEntry = PersistenceUtil.convert(mEntry, factory);
         return null;
      }
   }
}
