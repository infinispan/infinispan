package org.infinispan.query.affinity;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.engine.service.spi.ServiceManager;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.WorkerBuildContext;
import org.hibernate.search.store.DirectoryProvider;
import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;
import org.infinispan.hibernate.search.spi.InfinispanDirectoryProvider;
import org.infinispan.lucene.impl.DirectoryExtensions;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.query.backend.ComponentRegistryService;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * {@link org.hibernate.search.indexes.spi.IndexManager} that splits the index into shards.
 *
 * @author gustavonalle
 * @since 8.2
 */
public class AffinityIndexManager extends DirectoryBasedIndexManager implements OperationFailedHandler {

   private static final Log log = LogFactory.getLog(AffinityIndexManager.class, Log.class);
   private static final int POLL_WAIT = 100;

   private RpcManager rpcManager;
   private DistributionManager distributionManager;
   private KeyTransformationHandler keyTransformationHandler;
   private Cache cache;

   private volatile boolean hasOwnership = true;
   private volatile Address nextOwner = null;

   private final ReadWriteLock flushLock = new ReentrantReadWriteLock();
   private final Lock writeLock = flushLock.writeLock();
   private final Lock readLock = flushLock.readLock();
   private ExecutorService asyncExecutor;


   @Override
   public void lockObtainFailed(List<LuceneWork> failingOperations) {
      logDebug("Operation %s failed due to lock already in used", failingOperations);
      try {
         Thread.sleep(POLL_WAIT);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      Address lockHolder = getLockHolder(getIndexName());
      clearIfNeeded(lockHolder);
      if (failingOperations.stream().anyMatch(w -> w.getIdInString() != null)) {
         logDebug("Retrying operations %s", failingOperations);
         CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
            performOperations(failingOperations, null);
            return null;
         }, asyncExecutor);
         future.whenComplete((aVoid, throwable) -> {
            if (throwable == null) {
               logDebug("Operation completed");
            } else {
               log.errorf(throwable, "Error reapplying operation");
            }
         });
      }

   }

   private void clearIfNeeded(Address lockHolder) {
      if (!rpcManager.getMembers().contains(lockHolder)) {
         final Directory directory = getDirectoryProvider().getDirectory();
         log.debug("Forcing clear of index lock");
         ((DirectoryExtensions) directory).forceUnlock(IndexWriter.WRITE_LOCK_NAME);
      }
   }

   @Override
   public void initialize(String indexName, Properties properties, Similarity similarity, WorkerBuildContext buildContext) {
      super.initialize(indexName, properties, similarity, buildContext);
      ServiceManager serviceManager = buildContext.getServiceManager();
      ComponentRegistryService componentRegistryService = serviceManager.requestService(ComponentRegistryService.class);
      ComponentRegistry componentRegistry = componentRegistryService.getComponentRegistry();
      asyncExecutor = componentRegistry.getComponent(ExecutorService.class, ASYNC_OPERATIONS_EXECUTOR);
      distributionManager = componentRegistry.getComponent(DistributionManager.class);
      rpcManager = componentRegistry.getComponent(RpcManager.class);
      cache = componentRegistry.getComponent(Cache.class);
      keyTransformationHandler = componentRegistry.getComponent(QueryInterceptor.class).getKeyTransformationHandler();
      SearchIntegrator component = componentRegistry.getComponent(SearchIntegrator.class);
      AffinityErrorHandler errorHandler = (AffinityErrorHandler) component.getErrorHandler();
      cache.addListener(new TopologyChangeListener());
      errorHandler.initialize(this);
   }

   private void handleOwnershipLost(Address newOwner) {
      writeLock.lock();
      try {
         logDebug("Ownership lost, closing index manager");
         nextOwner = newOwner;
         flushAndReleaseResources();
         hasOwnership = false;
      } finally {
         writeLock.unlock();
      }
   }

   private void handleOwnershipAcquired() {
      writeLock.lock();
      try {
         logDebug("Ownership acquired");
         nextOwner = null;
         hasOwnership = true;
      } finally {
         writeLock.unlock();
      }
   }

   @Override
   public void flushAndReleaseResources() {
      Address lockHolder = getLockHolder(getIndexName());
      if (lockHolder != null) {
         InfinispanDirectoryProvider directoryProvider = (InfinispanDirectoryProvider) getDirectoryProvider();
         int activeDeleteTasks = directoryProvider.getActiveDeleteTasks();
         boolean wasInterrupted = false;
         while (activeDeleteTasks > 0) {
            try {
               Thread.sleep(POLL_WAIT);
               logDebug("Waiting for pending delete tasks, remaining: %s", activeDeleteTasks);
               activeDeleteTasks = directoryProvider.getActiveDeleteTasks();
            } catch (InterruptedException e) {
               wasInterrupted = true;
            }
         }
         if (wasInterrupted) {
            Thread.currentThread().interrupt();
         }
         logDebug("Flushing directory provider");
         super.flushAndReleaseResources();
      }
   }

   private Object stringToKey(String key) {
      return keyTransformationHandler.stringToKey(key, cache.getAdvancedCache().getClassLoader());
   }

   private int getSegment(Object key) {
      return distributionManager.getConsistentHash().getSegment(key);
   }

   private Address getLockHolder(String indexName) {
      InfinispanDirectoryProvider directoryProvider = (InfinispanDirectoryProvider) getDirectoryProvider();
      return directoryProvider.getLockOwner(indexName, IndexWriter.WRITE_LOCK_NAME);
   }

   private Address getLocation(LuceneWork work) {
      Address localAddress = rpcManager.getAddress();
      if (work.getIdInString() == null) {
         return localAddress;
      }
      int segment = getSegment(stringToKey(work.getIdInString()));
      Address lockHolder = getLockHolder(replaceShard(getIndexName(), segment));
      logDebug("Lock holder for %d is %s", segment, lockHolder);
      Address destination;
      if (!hasOwnership) {
         logDebug("Lost ownership, new owner is %s", nextOwner);
         destination = nextOwner;
      } else {
         destination = (lockHolder != null && !localAddress.equals(lockHolder)) ? lockHolder : localAddress;
      }
      return destination;
   }

   @SafeVarargs
   private final <T> List<T> newList(T... elements) {
      List<T> list = new ArrayList<>(elements.length);
      Collections.addAll(list, elements);
      return list;
   }

   private Map<Address, List<LuceneWork>> partitionWorkByAddress(List<LuceneWork> works) {
      return works.stream().collect(Collectors.toMap(this::getLocation, this::newList, (w1, w2) -> {
         w1.addAll(w2);
         return w1;
      }));
   }

   @Override
   public void performOperations(List<LuceneWork> workList, IndexingMonitor monitor) {
      if (this.cache.getCacheConfiguration().clustering().cacheMode().isClustered()) {
         readLock.lock();
         try {
            Address localAddress = rpcManager.getAddress();
            Map<Address, List<LuceneWork>> workByAddress = partitionWorkByAddress(workList);
            logDebug("Applying work @ %s, workMap is %s", localAddress, workByAddress);
            List<LuceneWork> localWork = workByAddress.get(localAddress);
            if (localWork != null && !localWork.isEmpty()) {
               logDebug("About to apply work locally %s, hasOwnership=%s", localWork, hasOwnership);
               super.performOperations(localWork, monitor);
               logDebug("Work applied");
               workByAddress.remove(localAddress);
            }
            workByAddress.entrySet().forEach(entry -> this.sendWork(entry.getValue(), entry.getKey()));
         } finally {
            readLock.unlock();
         }
      } else {
         super.performOperations(workList, monitor);
      }
   }

   private void sendWork(List<LuceneWork> works, Address destination) {
      AffinityUpdateCommand indexUpdateCommand = new AffinityUpdateCommand(ByteString.fromString(cache.getName()));
      byte[] serializedModel = getSerializer().toSerializedModel(works);
      indexUpdateCommand.setSerializedWorkList(serializedModel);
      List<Address> dest = Collections.singletonList(destination);
      log.debugf("Sending works %s to %s", works, dest);
      CompletableFuture<Map<Address, Response>> result
              = rpcManager.invokeRemotelyAsync(dest, indexUpdateCommand, rpcManager.getDefaultRpcOptions(false));
      result.whenComplete((responses, error) -> {
         if (error != null) {
            log.error("Error forwarding index job", error);
         }
      });
   }

   @Override
   protected DirectoryProvider<?> createDirectoryProvider(String indexName, Properties cfg, WorkerBuildContext buildContext) {
      String shardName = indexName.substring(indexName.lastIndexOf(".") + 1);

      InfinispanDirectoryProvider directoryProvider = new InfinispanDirectoryProvider(Integer.valueOf(shardName));
      directoryProvider.initialize(indexName, cfg, buildContext);

      return directoryProvider;
   }

   private String replaceShard(String indexName, int newSegment) {
      return indexName.substring(0, indexName.lastIndexOf(".") + 1).concat(String.valueOf(newSegment));
   }

   private void logDebug(String format, Object... params) {
      if (log.isDebugEnabled()) log.debugf(format, params);
   }

   @Listener
   @SuppressWarnings("unused")
   private class TopologyChangeListener {
      private final int segment;

      TopologyChangeListener() {
         String indexName = getIndexName();
         segment = AffinityShardIdentifierProvider.getSegment(getIndexName());
      }

      @TopologyChanged
      public void onTopologyChange(TopologyChangedEvent<?, ?> tce) {
         if (!tce.isPre()) {
            logDebug("Topology changed notification for %s: %s", getIndexName(), tce);
            Address localAddress = rpcManager.getAddress();
            Address previousOwner = tce.getConsistentHashAtStart().locatePrimaryOwnerForSegment(segment);
            Address newOwner = tce.getConsistentHashAtEnd().locatePrimaryOwnerForSegment(segment);
            if (previousOwner.equals(localAddress) && !newOwner.equals(localAddress)) {
               handleOwnershipLost(newOwner);
            } else if (!previousOwner.equals(localAddress) && newOwner.equals(localAddress)) {
               handleOwnershipAcquired();
            }
         }
      }
   }
}
