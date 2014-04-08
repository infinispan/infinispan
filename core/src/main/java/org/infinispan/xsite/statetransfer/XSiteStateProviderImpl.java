package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.RetryOnFailureXSiteCommand;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.persistence.spi.AdvancedCacheLoader.CacheLoaderTask;
import static org.infinispan.persistence.spi.AdvancedCacheLoader.TaskContext;
import static org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.MaxRetriesPolicy;
import static org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.RetryPolicy;
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

   private final ConcurrentMap<String, StatePushTask> runningStateTransfer;

   private DataContainer<?, ?> dataContainer;
   private PersistenceManager persistenceManager;
   private ClusteringDependentLogic clusteringDependentLogic;
   private CommandsFactory commandsFactory;
   private RpcManager rpcManager;
   private ExecutorService executorService;
   private Configuration configuration;
   private XSiteStateTransferManager stateTransferManager;
   private StateTransferLock stateTransferLock;

   public XSiteStateProviderImpl() {
      runningStateTransfer = CollectionFactory.makeConcurrentMap();
   }

   @Inject
   public void inject(DataContainer dataContainer, PersistenceManager persistenceManager, RpcManager rpcManager,
                      ClusteringDependentLogic clusteringDependentLogic, CommandsFactory commandsFactory,
                      @ComponentName(value = ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService,
                      Configuration configuration, XSiteStateTransferManager xSiteStateTransferManager,
                      StateTransferLock stateTransferLock) {
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.executorService = executorService;
      this.configuration = configuration;
      this.stateTransferManager = xSiteStateTransferManager;
      this.stateTransferLock = stateTransferLock;
   }

   @Override
   public void startStateTransfer(String siteName, Address origin, int minTopologyId) {
      XSiteStateTransferConfiguration stateTransferConfiguration = null;
      for (BackupConfiguration backupConfiguration : configuration.sites().allBackups()) {
         if (backupConfiguration.site().equals(siteName)) {
            stateTransferConfiguration = backupConfiguration.stateTransfer();
            break;
         }
      }

      if (stateTransferConfiguration == null) {
         throw new CacheException("Unable to start X-Site State Transfer! Backup configuration not found for " +
                                        siteName + "!");
      }
      StatePushTask task = new StatePushTask(siteName, origin, stateTransferConfiguration, minTopologyId);
      if (runningStateTransfer.putIfAbsent(siteName, task) == null) {
         if (debug) {
            log.debugf("Starting state transfer to site '%s'", siteName);
         }
         executorService.execute(task);
      } else if (debug) {
         log.debugf("Do not start state transfer to site '%s'. It has already started!", siteName);
      }

      //in case of the coordinator leaves before the command is processed!
      if (rpcManager.getAddress().equals(rpcManager.getMembers().get(0)) && !rpcManager.getMembers().contains(origin)) {
         stateTransferManager.becomeCoordinator(siteName);
      }
   }

   @Override
   public void cancelStateTransfer(String siteName) {
      StatePushTask task = runningStateTransfer.remove(siteName);
      if (task != null) {
         task.canceled = true;
      }
   }

   @Override
   public Collection<String> getCurrentStateSending() {
      Collection<String> sending = new ArrayList<>(runningStateTransfer.size());
      for (Map.Entry<String, StatePushTask> entry : runningStateTransfer.entrySet()) {
         if (!entry.getValue().finished) {
            sending.add(entry.getKey());
         }
      }
      return sending;
   }

   @Override
   public Collection<String> getSitesMissingCoordinator(Collection<Address> currentMembers) {
      final Collection<String> stateTransferNeedsNewCoordinator = new ArrayList<>(runningStateTransfer.size());
      for (Map.Entry<String, StatePushTask> entry : runningStateTransfer.entrySet()) {
         if (!currentMembers.contains(entry.getValue().origin)) {
            stateTransferNeedsNewCoordinator.add(entry.getKey());
         }
      }
      return stateTransferNeedsNewCoordinator;
   }

   private void notifyStateTransferEnd(final String siteName, final Address origin, final boolean error) {
      if (rpcManager.getAddress().equals(origin)) {
         executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               try {
                  stateTransferManager.notifyStatePushFinished(siteName, origin, !error);
               } catch (Throwable throwable) {
                  //ignored
               }
               return null;
            }
         });
      } else {
         XSiteStateTransferControlCommand command = commandsFactory.buildXSiteStateTransferControlCommand(FINISH_SEND, siteName);
         command.setStatusOk(!error);
         rpcManager.invokeRemotely(Collections.singleton(origin), command, rpcManager.getDefaultRpcOptions(false));
      }
   }

   private boolean shouldSendKey(Object key) {
      return clusteringDependentLogic.localNodeIsPrimaryOwner(key);
   }

   private void sendFromSharedBuffer(XSiteBackup xSiteBackup, List<XSiteState> sharedBuffer, StatePushTask task) throws Throwable {
      if (sharedBuffer.size() == 0) {
         return;
      }
      XSiteState[] privateBuffer = sharedBuffer.toArray(new XSiteState[sharedBuffer.size()]);

      if (trace) {
         log.debugf("Sending chunk to site '%s'. Chunk contains %s", xSiteBackup.getSiteName(),
                    Arrays.toString(privateBuffer));
      } else if (debug) {
         log.debugf("Sending chunk to site '%s'. Chunk has %s keys.", xSiteBackup.getSiteName(), privateBuffer.length);
      }

      XSiteStatePushCommand command = commandsFactory.buildXSiteStatePushCommand(privateBuffer);
      RetryOnFailureXSiteCommand remoteSite = RetryOnFailureXSiteCommand.newInstance(xSiteBackup, command, task.retryPolicy);
      remoteSite.execute(rpcManager.getTransport(), task.waitTime, TimeUnit.MILLISECONDS);
   }

   private void waitForTopology(int topologyId) throws InterruptedException {
      stateTransferLock.waitForTopology(topologyId, 1, TimeUnit.DAYS);
   }

   private class StatePushTask implements Runnable {

      private final XSiteBackup xSiteBackup;
      private final int chunkSize;
      private final Address origin;
      private final RetryPolicy retryPolicy;
      private final long waitTime;
      private final int minTopologyId;
      private volatile boolean finished;
      private volatile boolean canceled;
      private boolean error;

      public StatePushTask(String siteName, Address origin, XSiteStateTransferConfiguration configuration, int minTopologyId) {
         this.minTopologyId = minTopologyId;
         this.chunkSize = configuration.chunkSize();
         this.waitTime = configuration.waitTime();
         this.retryPolicy = new MaxRetriesPolicy(configuration.maxRetries());
         this.origin = origin;
         this.xSiteBackup = new XSiteBackup(siteName, true, configuration.timeout());
         this.canceled = false;
         this.finished = false;
         this.error = false;
      }

      @Override
      public void run() {
         try {
            if (debug) {
               log.debugf("[X-Site State Transfer - %s] wait for min topology %s", xSiteBackup.getSiteName(), minTopologyId);
            }

            waitForTopology(minTopologyId);

            final List<XSiteState> chunk = new ArrayList<>(chunkSize <= 0 ? DEFAULT_CHUNK_SIZE : chunkSize);

            if (debug) {
               log.debugf("[X-Site State Transfer - %s] start DataContainer iteration", xSiteBackup.getSiteName());
            }

            for (InternalCacheEntry ice : dataContainer) {
               if (canceled) {
                  log.debugf("[X-Site State Transfer - %s] State transfer canceled!", xSiteBackup.getSiteName());
                  return;
               }
               if (chunkSize > 0 && chunk.size() == chunkSize) {
                  try {
                     sendFromSharedBuffer(xSiteBackup, chunk, this);
                  } catch (Throwable t) {
                     error = true;
                     log.unableToSendXSiteState(xSiteBackup.getSiteName(), t);
                     return;
                  }
                  chunk.clear();
               }
               if (shouldSendKey(ice.getKey())) {
                  if (trace) {
                     log.tracef("Added key '%s' to current chunk", ice.getKey());
                  }
                  chunk.add(XSiteState.fromDataContainer(ice));
               }
            }

            if (canceled) {
               return;
            }
            if (chunk.size() > 0) {
               try {
                  sendFromSharedBuffer(xSiteBackup, chunk, this);
               } catch (Throwable t) {
                  error = true;
                  log.unableToSendXSiteState(xSiteBackup.getSiteName(), t);
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
               KeyFilter<Object> filter = new CacheLoaderFilter<>(new ReadOnlyDataContainerBackedKeySet(dataContainer));
               StateTransferCacheLoaderTask task = new StateTransferCacheLoaderTask(xSiteBackup, chunk, this);
               try {
                  stProvider.process(filter, task, EXECUTOR_SERVICE, true, true);
                  if (canceled) {
                     log.debugf("[X-Site State Transfer - %s] State transfer canceled!", xSiteBackup.getSiteName());
                     return;
                  }
                  task.sendRemainingState();
               } catch (CacheException e) {
                  error = true;
                  log.failedLoadingKeysFromCacheStore(e);
                  return;
               } catch (Throwable t) {
                  error = true;
                  log.unableToSendXSiteState(xSiteBackup.getSiteName(), t);
                  return;
               }
               if (debug) {
                  log.debugf("[X-Site State Transfer - %s] finish Persistence iteration", xSiteBackup.getSiteName());
               }
            } else if (debug) {
               log.debugf("[X-Site State Transfer - %s] skip Persistence iteration", xSiteBackup.getSiteName());
            }
         } catch (InterruptedException e) {
            error = true;
            log.unableToSendXSiteState(xSiteBackup.getSiteName(), e);
         } finally {
            finished = true;
            log.debugf("[X-Site State Transfer - %s] State transfer finished!", xSiteBackup.getSiteName());
            if (!canceled) {
               notifyStateTransferEnd(xSiteBackup.getSiteName(), origin, error);
            }
         }
      }

      @Override
      public String toString() {
         return "StatePushTask{" +
               "origin=" + origin +
               ", canceled=" + canceled +
               '}';
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
      private final StatePushTask task;

      private StateTransferCacheLoaderTask(XSiteBackup xSiteBackup, List<XSiteState> chunk, StatePushTask task) {
         this.xSiteBackup = xSiteBackup;
         this.chunk = chunk;
         this.task = task;
      }

      @Override
      public void processEntry(MarshalledEntry<Object, Object> marshalledEntry, TaskContext taskContext)
            throws InterruptedException {
         if (task.canceled) {
            taskContext.stop();
            log.debugf("[X-Site State Transfer - %s] State transfer canceled!", xSiteBackup.getSiteName());
            return;
         }
         if (task.chunkSize > 0 && chunk.size() == task.chunkSize) {
            try {
               sendFromSharedBuffer(xSiteBackup, chunk, task);
            } catch (Throwable t) {
               log.unableToSendXSiteState(xSiteBackup.getSiteName(), t);
               taskContext.stop();
            }
            chunk.clear();
         }

         chunk.add(XSiteState.fromCacheLoader(marshalledEntry));
      }

      public void sendRemainingState() throws Throwable {
         if (chunk.size() > 0) {
            sendFromSharedBuffer(xSiteBackup, chunk, task);
         }
      }
   }
}
