package org.infinispan.query.affinity;

import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.similarities.Similarity;
import org.hibernate.search.backend.BackendFactory;
import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.engine.service.spi.ServiceManager;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.WorkerBuildContext;
import org.hibernate.search.store.DirectoryProvider;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.search.spi.InfinispanDirectoryProvider;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.Listener.Observation;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.query.backend.ComponentRegistryService;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.backend.TransactionHelper;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link org.hibernate.search.indexes.spi.IndexManager} that splits the index into shards.
 *
 * @author gustavonalle
 * @since 8.2
 */
@Listener(observation = Observation.POST)
public class AffinityIndexManager extends DirectoryBasedIndexManager {

   private static final Log log = LogFactory.getLog(AffinityIndexManager.class, Log.class);
   private static final long POLL_WAIT = 1000L;

   private KeyTransformationHandler keyTransformationHandler;
   private Cache<?, ?> cache;
   private boolean isClustered;

   private final ReadWriteLock flushLock = new ReentrantReadWriteLock();
   private final Lock writeLock = flushLock.writeLock();
   private final Lock readLock = flushLock.readLock();

   private ShardAllocatorManager shardAllocatorManager;
   private TransactionHelper transactionHelper;
   private SearchIntegrator searchIntegrator;
   private String shardId;
   private boolean isAsync;
   private ShardAddress localShardAddress;
   private LuceneWorkDispatcher luceneWorkDispatcher;
   private WorkPartitioner workPartitioner;

   @Override
   public void initialize(String indexName, Properties properties, Similarity similarity,
                          WorkerBuildContext buildContext) {
      ServiceManager serviceManager = buildContext.getServiceManager();
      ComponentRegistryService componentRegistryService = serviceManager.requestService(ComponentRegistryService.class);
      ComponentRegistry componentRegistry = componentRegistryService.getComponentRegistry();
      transactionHelper = new TransactionHelper(componentRegistry.getComponent(TransactionManager.class));
      shardId = extractShardName(indexName);
      Transaction tx = transactionHelper.suspendTxIfExists();
      try {
         super.initialize(indexName, properties, similarity, buildContext);
      } finally {
         transactionHelper.resume(tx);
      }
      cache = componentRegistry.getComponent(Cache.class);
      isClustered = cache.getCacheConfiguration().clustering().cacheMode().isClustered();
      keyTransformationHandler = componentRegistry.getComponent(QueryInterceptor.class).getKeyTransformationHandler();
      shardAllocatorManager = componentRegistry.getComponent(ShardAllocatorManager.class);
      searchIntegrator = componentRegistry.getComponent(SearchIntegrator.class);
      isAsync = !BackendFactory.isConfiguredAsSync(properties);
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      localShardAddress = new ShardAddress(shardId, rpcManager != null ? rpcManager.getAddress() : LocalModeAddress.INSTANCE);
      ExecutorService asyncExecutor = componentRegistry.getComponent(ExecutorService.class, ASYNC_OPERATIONS_EXECUTOR);
      luceneWorkDispatcher = new LuceneWorkDispatcher(this, rpcManager);
      workPartitioner = new WorkPartitioner(this, shardAllocatorManager);
      AffinityErrorHandler errorHandler = (AffinityErrorHandler) searchIntegrator.getErrorHandler();
      errorHandler.initialize(rpcManager, asyncExecutor);
      cache.addListener(this);
   }

   private void handleOwnershipLost() {
      writeLock.lock();
      try {
         log.debugf("Ownership of %s lost to '%s', closing index manager",
               getIndexName(), shardAllocatorManager.getOwner(shardId));
         flushAndReleaseResources();
      } finally {
         writeLock.unlock();
      }
   }

   @Override
   public void flushAndReleaseResources() {
      InfinispanDirectoryProvider directoryProvider = (InfinispanDirectoryProvider) getDirectoryProvider();
      int activeDeleteTasks = directoryProvider.pendingDeleteTasks();
      boolean wasInterrupted = false;
      long endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(POLL_WAIT);
      while (activeDeleteTasks > 0 && endTime - System.nanoTime() > 0) {
         try {
            Thread.sleep(10);
            log.debugf("Waiting for pending delete tasks, remaining: %s", activeDeleteTasks);
            activeDeleteTasks = directoryProvider.pendingDeleteTasks();
         } catch (InterruptedException ignored) {
            wasInterrupted = true;
         }
      }
      if (wasInterrupted) {
         Thread.currentThread().interrupt();
      }
      log.debugf("Flushing directory provider at %s on %s", getIndexName(), localShardAddress);
      super.flushAndReleaseResources();
   }

   Object stringToKey(String key) {
      return keyTransformationHandler.stringToKey(key);
   }

   Address getLockHolder(String indexName, String affinityId) {
      log.debugf("Getting lock holder for %s", indexName);
      Transaction tx = transactionHelper.suspendTxIfExists();
      try {
         InfinispanDirectoryProvider directoryProvider = (InfinispanDirectoryProvider) getDirectoryProvider();
         return directoryProvider.getLockOwner(indexName, Integer.valueOf(affinityId), IndexWriter.WRITE_LOCK_NAME);
      } finally {
         transactionHelper.resume(tx);
      }
   }

   Address getLockHolder() {
      return getLockHolder(getIndexName(), shardId);
   }

   @Override
   public void performOperations(List<LuceneWork> workList, IndexingMonitor monitor) {
      performOperations(workList, monitor, true, false);
   }

   private void checkOwnership() {
      log.debugf("Checking ownership at %s", localShardAddress);
      Address primaryOwner = shardAllocatorManager.getOwner(shardId);
      if (!localShardAddress.getAddress().equals(primaryOwner)) {
         log.debugf("%s is not owner of %s anymore, releasing resources", localShardAddress, getIndexName());
         handleOwnershipLost();
      }
   }

   void performOperations(List<LuceneWork> workList, IndexingMonitor monitor, boolean originLocal, boolean isRetry) {
      if (isClustered) {
         Map<ShardAddress, List<LuceneWork>> workByAddress = workPartitioner.partitionWorkByAddress(workList, originLocal,
               isRetry);
         readLock.lock();
         try {
            log.debugf("Applying work @ %s, workMap is %s", localShardAddress, workByAddress);
            List<LuceneWork> localWork = workByAddress.get(localShardAddress);
            if (localWork != null && !localWork.isEmpty()) {
               log.debugf("About to apply local work %s (index %s) at %s", localWork, getIndexName(), localShardAddress);
               super.performOperations(localWork, monitor);
               log.debugf("Work %s applied at %s", localWork, localShardAddress);
               workByAddress.remove(localShardAddress);
            }
         } finally {
            readLock.unlock();
         }
         workByAddress.forEach((key, value) -> luceneWorkDispatcher.dispatch(value, key, originLocal));
         if (isRetry || !originLocal) {
            checkOwnership();
         }
      } else {

         super.performOperations(workList, monitor);
      }
   }

   private static String extractShardName(String indexName) {
      int idx = indexName.lastIndexOf('.');
      return idx == -1 ? "0" : indexName.substring(idx + 1);
   }

   @Override
   protected DirectoryProvider<?> createDirectoryProvider(String indexName, Properties cfg, WorkerBuildContext buildContext) {
      InfinispanDirectoryProvider directoryProvider = new InfinispanDirectoryProvider(Integer.valueOf(shardId));
      directoryProvider.initialize(indexName, cfg, buildContext);

      return directoryProvider;
   }

   ShardAddress getLocalShardAddress() {
      return localShardAddress;
   }

   KeyTransformationHandler getKeyTransformationHandler() {
      return keyTransformationHandler;
   }

   String getCacheName() {
      return cache.getName();
   }

   boolean isAsync() {
      return isAsync;
   }

   SearchIntegrator getSearchIntegrator() {
      return searchIntegrator;
   }

   @TopologyChanged
   @SuppressWarnings("unused")
   public void onTopologyChange(TopologyChangedEvent<?, ?> tce) {
      log.debugf("Topology changed notification for %s: %s", getIndexName(), tce);
      boolean ownershipChanged = shardAllocatorManager.isOwnershipChanged(tce, getIndexName());
      log.debugf("Ownership changed? %s,", ownershipChanged);
      if (ownershipChanged) {
         handleOwnershipLost();
      }
   }
}
