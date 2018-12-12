package org.infinispan.xsite.statetransfer;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.MaxRetriesPolicy;
import static org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.RetryPolicy;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.StateTransferControl.FINISH_SEND;

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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.RetryOnFailureXSiteCommand;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;

import io.reactivex.Flowable;

/**
 * It contains the logic to send state to another site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateProviderImpl implements XSiteStateProvider {

   private static final int DEFAULT_CHUNK_SIZE = 1024;
   private static final Log log = LogFactory.getLog(XSiteStateProviderImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean debug = log.isDebugEnabled();

   private final ConcurrentMap<String, StatePushTask> runningStateTransfer;

   @Inject private InternalDataContainer<?, ?> dataContainer;
   @Inject private PersistenceManager persistenceManager;
   @Inject private ClusteringDependentLogic clusteringDependentLogic;
   @Inject private CommandsFactory commandsFactory;
   @Inject private RpcManager rpcManager;
   @Inject @ComponentName(value = ASYNC_TRANSPORT_EXECUTOR)
   private ExecutorService executorService;
   @Inject private Configuration configuration;
   @Inject private ComponentRef<XSiteStateTransferManager> stateTransferManager;
   @Inject private StateTransferLock stateTransferLock;

   public XSiteStateProviderImpl() {
      runningStateTransfer = CollectionFactory.makeConcurrentMap();
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
         stateTransferManager.running().becomeCoordinator(siteName);
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
         executorService.submit((Callable<Void>) () -> {
            try {
               stateTransferManager.running().notifyStatePushFinished(siteName, origin, !error);
            } catch (Throwable throwable) {
               //ignored
            }
            return null;
         });
      } else {
         XSiteStateTransferControlCommand command = commandsFactory.buildXSiteStateTransferControlCommand(FINISH_SEND, siteName);
         command.setStatusOk(!error);
         rpcManager.invokeRemotely(Collections.singleton(origin), command, rpcManager.getDefaultRpcOptions(false));
      }
   }

   private boolean shouldSendKey(Object key) {
      return clusteringDependentLogic.getCacheTopology().getDistribution(key).isPrimary();
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

      XSiteStatePushCommand command = commandsFactory.buildXSiteStatePushCommand(privateBuffer, xSiteBackup.getTimeout());
      RetryOnFailureXSiteCommand remoteSite = RetryOnFailureXSiteCommand.newInstance(xSiteBackup, command, task.retryPolicy);
      remoteSite.execute(rpcManager, task.waitTime, TimeUnit.MILLISECONDS);
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

            CompletableFutures.await(stateTransferLock.topologyFuture(minTopologyId));

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
               try {
                  Flowable.fromPublisher(stProvider.entryPublisher(k -> shouldSendKey(k) && !dataContainer.containsKey(k), true, true))
                        .map(XSiteState::fromCacheLoader)
                        .takeUntil(l -> canceled)
                        .buffer(chunkSize <= 0 ? Integer.MAX_VALUE : chunkSize)
                        // We want the CacheException to be thrown to the catch block
                        .blockingForEach(l -> {
                           try {
                              sendFromSharedBuffer(xSiteBackup, l, this);
                           } catch (Throwable throwable) {
                              // This will terminate the flowable early
                              throw new CacheException(throwable);
                           }
                        });

                  if (canceled) {
                     log.debugf("[X-Site State Transfer - %s] State transfer canceled!", xSiteBackup.getSiteName());
                     return;
                  }
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
         } catch (Throwable e) {
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
}
