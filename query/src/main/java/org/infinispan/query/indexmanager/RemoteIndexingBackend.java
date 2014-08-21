package org.infinispan.query.indexmanager;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.LogFactory;

/**
 * The IndexingBackend which forwards all operations to a different node
 * using Infinispan's custom commands.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
final class RemoteIndexingBackend implements IndexingBackend {

   private static final Log log = LogFactory.getLog(RemoteIndexingBackend.class, Log.class);

   private final String cacheName;
   private final String indexName;
   private final Collection<Address> recipients;
   private final RpcManager rpcManager;
   private final Address masterAddress;

   public RemoteIndexingBackend(String cacheName, RpcManager rpcManager, String indexName, Address masterAddress) {
      this.cacheName = cacheName;
      this.rpcManager = rpcManager;
      this.indexName = indexName;
      this.masterAddress = masterAddress;
      this.recipients = Collections.singleton(masterAddress);
   }

   @Override
   public void flushAndClose(IndexingBackend replacement) {
      // no-op: this implementation is essentially stateless.
   }

   @Override
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      IndexUpdateCommand command = new IndexUpdateCommand(cacheName);
      //Use Search's custom Avro based serializer as it includes support for back/future compatibility
      byte[] serializedModel = indexManager.getSerializer().toSerializedModel(workList);
      command.setSerializedWorkList(serializedModel);
      command.setIndexName(this.indexName);
      log.applyingChangeListRemotely(workList);
      sendCommand(command, workList);
   }

   @Override
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      final IndexUpdateStreamCommand streamCommand = new IndexUpdateStreamCommand(cacheName);
      final List<LuceneWork> operations = Collections.singletonList(singleOperation);
      final byte[] serializedModel = indexManager.getSerializer().toSerializedModel(operations);
      streamCommand.setSerializedWorkList(serializedModel);
      streamCommand.setIndexName(this.indexName);
      sendCommand(streamCommand, operations);
   }

   private void sendCommand(ReplicableCommand command, List<LuceneWork> workList) {
      rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(true));
      log.workListRemotedTo(workList, masterAddress);
   }

   @Override
   public boolean isMasterLocal() {
      return false;
   }

}
