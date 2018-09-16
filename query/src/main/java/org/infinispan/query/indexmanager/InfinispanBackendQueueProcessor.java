package org.infinispan.query.indexmanager;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;

import javax.transaction.TransactionManager;

import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.impl.lucene.WorkspaceHolder;
import org.hibernate.search.engine.service.spi.ServiceManager;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.WorkerBuildContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.search.spi.CacheManagerService;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.backend.ComponentRegistryService;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.logging.LogFactory;

/**
 * Adaptor to implement the Hibernate Search contract of a BackendQueueProcessor
 * while delegating to the cluster-aware components of Infinispan Query.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2014 Red Hat Inc.
 * @since 7.0
 */
final class InfinispanBackendQueueProcessor extends WorkspaceHolder {

   private static final Log log = LogFactory.getLog(InfinispanBackendQueueProcessor.class, Log.class);

   private ServiceManager serviceManager;
   private String indexName;
   private IndexManager indexManager;
   private SwitchingBackend forwardingBackend;

   @Override
   public void initialize(Properties props, WorkerBuildContext context, IndexManager indexManager) {
      this.indexManager = indexManager;
      LocalBackendFactory localBackendFactory = new SimpleLocalBackendFactory(indexManager, props, context);
      serviceManager = context.getServiceManager();
      CacheManagerService cacheManagerService = serviceManager.requestService(CacheManagerService.class);
      this.indexName = indexManager.getIndexName();
      ComponentRegistryService componentRegistryService = serviceManager.requestService(ComponentRegistryService.class);
      ComponentRegistry componentRegistry = componentRegistryService.getComponentRegistry();
      this.forwardingBackend = createForwardingBackend(props, componentRegistry, indexName, localBackendFactory, cacheManagerService, indexManager);
      log.commandsBackendInitialized(indexName);
   }

   private static SwitchingBackend createForwardingBackend(Properties props, ComponentRegistry componentRegistry, String indexName, LocalBackendFactory localBackendFactory, CacheManagerService cacheManagerService, IndexManager indexManager) {
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      if (rpcManager == null) {
         //non-clustered case:
         LocalOnlyBackend backend = new LocalOnlyBackend(localBackendFactory);
         backend.initialize();
         return backend;
      } else {
         EmbeddedCacheManager embeddedCacheManager = cacheManagerService.getEmbeddedCacheManager();
         TransactionManager transactionManager = componentRegistry.getComponent(TransactionManager.class);
         //The following cast is currently safe as the contract just changed from IndexManager to DirectoryBasedIndexManager
         //but it was changed as the intention is to evolve on this introducing non-Directory based IndexManager implementations.
         //FIXME: avoid the need for the cast or validate eagerly with a nicer error message.
         //https://issues.jboss.org/browse/ISPN-6212
         DirectoryBasedIndexManager directoryBasedIndexManager = (DirectoryBasedIndexManager) indexManager;
         IndexLockController lockControl = new IndexManagerBasedLockController(directoryBasedIndexManager, transactionManager);
         ClusteredSwitchingBackend backend = new ClusteredSwitchingBackend(props, componentRegistry, indexName, localBackendFactory, lockControl);
         backend.initialize();
         embeddedCacheManager.addListener(backend);
         return backend;
      }
   }

   @Override
   public void close() {
      forwardingBackend.shutdown();
      serviceManager.releaseService(CacheManagerService.class);
      serviceManager.releaseService(ComponentRegistryService.class);
      serviceManager = null;
   }

   @Override
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor) {
      try {
         forwardingBackend.getCurrentIndexingBackend().applyWork(workList, monitor, indexManager);
      } catch (SuspectException e) {
         forwardingBackend.refresh();
         forwardingBackend.getCurrentIndexingBackend().applyWork(workList, monitor, indexManager);
      }
   }

   @Override
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor) {
      forwardingBackend.getCurrentIndexingBackend()
            .applyStreamWork(singleOperation, monitor, indexManager);
   }

   @Override
   public Lock getExclusiveWriteLock() {
      throw new UnsupportedOperationException("Not Implementable: nonsense on a distributed index.");
   }

   @Override
   public void indexMappingChanged() {
      //FIXME implement me? Not sure it's needed.
   }

   boolean isMasterLocal() {
      return forwardingBackend.getCurrentIndexingBackend().isMasterLocal();
   }

}
