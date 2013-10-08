package org.infinispan.query.indexmanager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;

import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.spi.BackendQueueProcessor;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.hibernate.search.infinispan.CacheManagerServiceProvider;
import org.hibernate.search.spi.WorkerBuildContext;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.backend.ComponentRegistryServiceProvider;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An Hibernate Search backend module tailored to delegate index writing to
 * another node in the Infinispan cluster using custom remoting commands.
 * 
 * The master IndexWriter for each index is elected as a consistent hash
 * on the index name. Index shards in Hibernate Search have different names,
 * so this provides a primitive load balancing.
 * 
 * TODO Design a policy to deterministically try balance different shards of
 * the same index on different nodes.
 * 
* @author Sanne Grinovero
*/
public class InfinispanCommandsBackend implements BackendQueueProcessor {

   private static final Log log = LogFactory.getLog(InfinispanCommandsBackend.class, Log.class);

   private EmbeddedCacheManager cacheManager;
   private WorkerBuildContext context;
   private String indexName;
   private DistributionManager distributionManager;
   private RpcManager rpcManager;
   private String cacheName;
   private DirectoryBasedIndexManager indexManager;
   private HashSet<Class> knownTypes = new HashSet<Class>(10);

   @Override
   public void initialize(Properties props, WorkerBuildContext context, DirectoryBasedIndexManager indexManager) {
      this.context = context;
      this.indexManager = indexManager;
      this.cacheManager = context.requestService(CacheManagerServiceProvider.class);
      final ComponentRegistry componentsRegistry = context.requestService(ComponentRegistryServiceProvider.class);
      this.indexName = indexManager.getIndexName();
      this.rpcManager = componentsRegistry.getComponent(RpcManager.class);
      this.cacheName = componentsRegistry.getCacheName();
      this.distributionManager = componentsRegistry.getComponent(DistributionManager.class);
      log.commandsBackendInitialized(indexName);
   }

   @Override
   public void close() {
      context.releaseService(CacheManagerServiceProvider.class);
      context.releaseService(ComponentRegistryServiceProvider.class);
      context = null;
      cacheManager = null;
   }

   @Override
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor) {
      IndexUpdateCommand command = new IndexUpdateCommand(cacheName);
      //Use Search's custom Avro based serializer as it includes support for back/future compatibility
      byte[] serializedModel = indexManager.getSerializer().toSerializedModel(workList);
      command.setSerializedWorkList(serializedModel);
      command.setIndexName(this.indexName);
      sendCommand(command, workList);
   }

   private void sendCommand(ReplicableCommand command, List<LuceneWork> workList) {
      Address primaryNodeAddress = getPrimaryNodeAddress();
      Collection<Address> recipients = Collections.singleton(primaryNodeAddress);
      rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(true));
      log.workListRemotedTo(workList, primaryNodeAddress);
   }

   @Override
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor) {
      applyWork(Collections.singletonList(singleOperation), monitor);
   }

   @Override
   public Lock getExclusiveWriteLock() {
      throw new UnsupportedOperationException("Not Implementable: nonsense on a distributed index.");
   }

   @Override
   public void indexMappingChanged() {
      //FIXME implement me? Not sure it's needed.
   }

   public boolean isMasterLocal() {
      Transport transport = cacheManager.getTransport();
      if (transport == null) {
         return true;
      }
      else {
         final Address primaryLocation = getPrimaryNodeAddress();
         Address localAddress = transport.getAddress();
         return localAddress.equals(primaryLocation);
      }
   }

   /**
    * Returns the primary node for this index, or null
    * for non clustered configurations.
    */
   private Address getPrimaryNodeAddress() {
      Transport transport = cacheManager.getTransport();
      if (transport == null) {
         return null;
      }
      if (distributionManager == null) { //If cache is configured as REPL
         //TODO Make this decision making pluggable?
         List<Address> members = transport.getMembers();
         int elementIndex = (Math.abs(indexName.hashCode()) % members.size());
         return members.get(elementIndex);
      }
      else { //If cache is configured as DIST
         return distributionManager.getPrimaryLocation(indexName);
      }
   }

}
