package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.persistence.spi.AdvancedCacheLoader.*;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.StateTransferControl.FINISH_SEND;

/**
 * It contains the logic to send state to another site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateProviderImpl implements XSiteStateProvider {

   private static final int DEFAULT_CHUNK_SIZE = 1024;
   private static final ExecutorService EXECUTOR_SERVICE = new WithinThreadExecutor();
   private static final Log log = LogFactory.getLog(XSiteStateProviderImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean debug = log.isDebugEnabled();

   private final ConcurrentMap<String, StateProviderRunnable> runningStateTransfer;

   private DataContainer<Object, Object> dataContainer;
   private PersistenceManager persistenceManager;
   private ClusteringDependentLogic clusteringDependentLogic;
   private CommandsFactory commandsFactory;
   private RpcManager rpcManager;
   private ExecutorService executorService;
   private Configuration configuration;
   private XSiteStateTransferManager stateTransferManager;

   public XSiteStateProviderImpl() {
      runningStateTransfer = CollectionFactory.makeConcurrentMap();
   }

   @Inject
   public void inject(DataContainer dataContainer, PersistenceManager persistenceManager, RpcManager rpcManager,
                      ClusteringDependentLogic clusteringDependentLogic, CommandsFactory commandsFactory,
                      @ComponentName(value = ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService,
                      Configuration configuration, XSiteStateTransferManager stateTransferManager) {
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.executorService = executorService;
      this.configuration = configuration;
      this.stateTransferManager = stateTransferManager;
   }

   @Override
   public void startStateTransfer(String siteName, Address origin) {
      int chunkSize = 0;
      long timeout = 0;
      boolean found = false;
      for (BackupConfiguration backupConfiguration : configuration.sites().allBackups()) {
         if (backupConfiguration.site().equals(siteName)) {
            chunkSize = backupConfiguration.stateTransfer().chunkSize();
            timeout = backupConfiguration.stateTransfer().timeout();
            found = true;
            break;
         }
      }

      if (!found) {
         throw new CacheException("Unable to start X-Site State Transfer! Backup configuration not found for " +
                                        siteName + "!");
      }
      StateProviderRunnable runnable = new StateProviderRunnable(siteName, chunkSize, timeout, origin);
      if (runningStateTransfer.putIfAbsent(siteName, runnable) == null) {
         if (debug) {
            log.debugf("Starting state transfer to site '%s'", siteName);
         }
         executorService.execute(runnable);
      } else if (debug) {
         log.debugf("Do not start state transfer to site '%s'. It has already started!", siteName);
      }
   }

   @Override
   public void cancelStateTransfer(String siteName) {
      StateProviderRunnable runnable = runningStateTransfer.remove(siteName);
      if (runnable != null) {
         runnable.canceled.set(true);
      }
   }

   @Override
   public Collection<String> getCurrentStateSending() {
      return new ArrayList<String>(runningStateTransfer.keySet());
   }

   private void notifyStateTransferEnd(final String siteName, final Address origin) {
      runningStateTransfer.remove(siteName);
      if (rpcManager.getAddress().equals(origin)) {
         executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               try {
                  stateTransferManager.notifyStatePushFinished(siteName, origin);
               } catch (Throwable throwable) {
                  //ignored
               }
               return null;
            }
         });
      } else {
         ReplicableCommand command = commandsFactory.buildXSiteStateTransferControlCommand(FINISH_SEND, siteName);
         rpcManager.invokeRemotely(Collections.singleton(origin), command, rpcManager.getDefaultRpcOptions(false));
      }
   }

   private boolean shouldSendKey(Object key) {
      return clusteringDependentLogic.localNodeIsPrimaryOwner(key);
   }

   private void sendFromSharedBuffer(XSiteBackup xSiteBackup, List<XSiteState> sharedBuffer,
                                     Collection<BackupResponse> responses) throws Exception {
      if (sharedBuffer.size() == 0) {
         return;
      }
      XSiteState[] privateBuffer = sharedBuffer.toArray(new XSiteState[sharedBuffer.size()]);

      if (debug) {
         log.debugf("Sending chunk to site '%s'. Chunk has %s keys.", xSiteBackup.getSiteName(), privateBuffer.length);
      } else if (trace) {
         log.debugf("Sending chunk to site '%s'. Chunk contains %s", xSiteBackup.getSiteName(),
                    Arrays.toString(privateBuffer));
      }

      XSiteStatePushCommand command = commandsFactory.buildXSiteStatePushCommand(privateBuffer);
      responses.add(invokeRemotelyInRemoteSite(command, xSiteBackup));
   }

   private BackupResponse invokeRemotelyInRemoteSite(XSiteReplicateCommand command, XSiteBackup xSiteBackup) throws Exception {
      return rpcManager.getTransport().backupRemotely(Collections.singletonList(xSiteBackup), command);
   }

   private class StateProviderRunnable implements Runnable {

      private final XSiteBackup xSiteBackup;
      private final int chunkSize;
      private final Address origin;
      private final AtomicBoolean canceled;

      private StateProviderRunnable(String siteName, int chunkSize, long timeout, Address origin) {
         this.chunkSize = chunkSize;
         this.origin = origin;
         this.xSiteBackup = new XSiteBackup(siteName, true, timeout);
         this.canceled = new AtomicBoolean(false);
      }

      @Override
      public void run() {
         try {
            final List<XSiteState> chunk = new ArrayList<XSiteState>(chunkSize <= 0 ? DEFAULT_CHUNK_SIZE : chunkSize);
            final Queue<BackupResponse> backupResponseQueue = new LinkedList<BackupResponse>();

            if (debug) {
               log.debugf("[X-Site State Transfer - %s] start DataContainer iteration", xSiteBackup.getSiteName());
            }

            for (InternalCacheEntry ice : dataContainer) {
               if (canceled.get()) {
                  return;
               }
               if (chunkSize > 0 && chunk.size() == chunkSize) {
                  try {
                     sendFromSharedBuffer(xSiteBackup, chunk, backupResponseQueue);
                  } catch (Exception e) {
                     log.unableToSendXSiteState(xSiteBackup.getSiteName(), e);
                     return;
                  }
                  chunk.clear();
               }
               if (shouldSendKey(ice.getKey())) {
                  chunk.add(XSiteState.fromDataContainer(ice));
               }
            }

            if (canceled.get()) {
               return;
            }
            if (chunk.size() > 0) {
               try {
                  sendFromSharedBuffer(xSiteBackup, chunk, backupResponseQueue);
               } catch (Exception e) {
                  log.unableToSendXSiteState(xSiteBackup.getSiteName(), e);
                  return;
               }
            }

            if (debug) {
               log.debugf("[X-Site State Transfer - %s] finish DataContainer iteration", xSiteBackup.getSiteName());
            }

            @SuppressWarnings("unchecked")
            AdvancedCacheLoader<Object, Object> stProvider = persistenceManager.getStateTransferProvider();
            if (stProvider != null) {
               if (debug) {
                  log.debugf("[X-Site State Transfer - %s] start Persistence iteration", xSiteBackup.getSiteName());
               }
               KeyFilter<Object> filter = new CacheLoaderFilter(new ReadOnlyDataContainerBackedKeySet(dataContainer));
               StateTransferCacheLoaderTask task = new StateTransferCacheLoaderTask(xSiteBackup, chunk, chunkSize,
                                                                                    backupResponseQueue, canceled);
               try {
                  stProvider.process(filter, task, EXECUTOR_SERVICE, true, true);
                  if (canceled.get()) {
                     return;
                  }
                  task.sendRemainingState();
               } catch (CacheException e) {
                  log.failedLoadingKeysFromCacheStore(e);
               } catch (Exception e) {
                  log.unableToSendXSiteState(xSiteBackup.getSiteName(), e);
               }
               if (debug) {
                  log.debugf("[X-Site State Transfer - %s] finish Persistence iteration", xSiteBackup.getSiteName());
               }
            } else if (debug) {
               log.debugf("[X-Site State Transfer - %s] skip Persistence iteration", xSiteBackup.getSiteName());
            }

            BackupResponse response = backupResponseQueue.poll();
            while (response != null) {
               if (canceled.get()) {
                  return;
               }
               try {
                  response.waitForBackupToFinish();
               } catch (Exception e) {
                  log.unableToWaitForXSiteStateAcks(xSiteBackup.getSiteName(), e);
                  return;
               }
               response = backupResponseQueue.poll();
            }
         } finally {
            notifyStateTransferEnd(xSiteBackup.getSiteName(), origin);
         }
      }

   }

   private class CacheLoaderFilter<K> extends CollectionKeyFilter<K> {

      public CacheLoaderFilter(Collection<? extends K> rejectedKeys) {
         super(rejectedKeys);
      }

      @Override
      public boolean accept(K key) {
         return shouldSendKey(key) && super.accept(key);
      }
   }

   private class StateTransferCacheLoaderTask implements CacheLoaderTask<Object, Object> {

      private final List<XSiteState> chunk;
      private final XSiteBackup xSiteBackup;
      private final Collection<BackupResponse> responses;
      private final AtomicBoolean canceled;
      private final int chunkSize;

      private StateTransferCacheLoaderTask(XSiteBackup xSiteBackup, List<XSiteState> chunk, int chunkSize,
                                           Collection<BackupResponse> responses, AtomicBoolean canceled) {
         this.xSiteBackup = xSiteBackup;
         this.chunk = chunk;
         this.chunkSize = chunkSize;
         this.responses = responses;
         this.canceled = canceled;
      }

      @Override
      public void processEntry(MarshalledEntry<Object, Object> marshalledEntry, TaskContext taskContext)
            throws InterruptedException {
         if (canceled.get()) {
            taskContext.stop();
            return;
         }
         if (chunkSize > 0 && chunk.size() == chunkSize) {
            try {
               sendFromSharedBuffer(xSiteBackup, chunk, responses);
            } catch (Exception e) {
               log.unableToSendXSiteState(xSiteBackup.getSiteName(), e);
               taskContext.stop();
            }
            chunk.clear();
         }

         chunk.add(XSiteState.fromCacheLoader(marshalledEntry));
      }

      public void sendRemainingState() throws Exception {
         if (chunk.size() > 0) {
            sendFromSharedBuffer(xSiteBackup, chunk, responses);
         }
      }
   }
}
