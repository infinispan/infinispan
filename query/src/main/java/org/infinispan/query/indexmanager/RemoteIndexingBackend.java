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

   private final int GRACE_MILLISECONDS_FOR_REPLACEMENT = 4000;
   private final int POLLING_MILLISECONDS_FOR_REPLACEMENT = 4000;

   private final String cacheName;
   private final String indexName;
   private final Collection<Address> recipients;
   private final RpcManager rpcManager;
   private final Address masterAddress;
   private final boolean async;

   private volatile IndexingBackend replacement;

   public RemoteIndexingBackend(String cacheName, RpcManager rpcManager, String indexName, Address masterAddress, boolean async) {
      this.cacheName = cacheName;
      this.rpcManager = rpcManager;
      this.indexName = indexName;
      this.masterAddress = masterAddress;
      this.recipients = Collections.singleton(masterAddress);
      this.async = async;
   }

   @Override
   public void flushAndClose(IndexingBackend replacement) {
      if (replacement != null) {
         this.replacement = replacement;
      }
   }

   @Override
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      IndexUpdateCommand command = new IndexUpdateCommand(cacheName);
      //Use Search's custom Avro based serializer as it includes support for back/future compatibility
      byte[] serializedModel = indexManager.getSerializer().toSerializedModel(workList);
      command.setSerializedWorkList(serializedModel);
      command.setIndexName(this.indexName);
      try {
         log.applyingChangeListRemotely(workList);
         sendCommand(command, workList);
      } catch (Exception e) {
         waitForReplacementBackend();
         if (replacement != null) {
            replacement.applyWork(workList, monitor, indexManager);
         } else {
            throw e;
         }
      }
   }

   @Override
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      final IndexUpdateStreamCommand streamCommand = new IndexUpdateStreamCommand(cacheName);
      final List<LuceneWork> operations = Collections.singletonList(singleOperation);
      final byte[] serializedModel = indexManager.getSerializer().toSerializedModel(operations);
      streamCommand.setSerializedWorkList(serializedModel);
      streamCommand.setIndexName(this.indexName);
      try {
         sendCommand(streamCommand, operations);
      } catch (Exception e) {
         waitForReplacementBackend();
         if (replacement != null) {
            replacement.applyStreamWork(singleOperation, monitor, indexManager);
         } else {
            throw e;
         }
      }
   }

   /**
    * If some error happened, and a new IndexingBackend was provided, it is safe to
    * assume the error relates with the fact the current backend is no longer valid.
    * At this point we can still forward the work from the current stack to the next
    * backend, creating a linked list of forwards to the right backend:
    */
   private void waitForReplacementBackend() {
      int waitedMilliseconds = 0;
      try {
         while (replacement != null) {
            if (waitedMilliseconds >= GRACE_MILLISECONDS_FOR_REPLACEMENT) {
               return;
            }
            Thread.sleep(POLLING_MILLISECONDS_FOR_REPLACEMENT);
            waitedMilliseconds += POLLING_MILLISECONDS_FOR_REPLACEMENT;
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }

   private void sendCommand(ReplicableCommand command, List<LuceneWork> workList) {
      rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(!async));
      log.workListRemotedTo(workList, masterAddress);
   }

   @Override
   public boolean isMasterLocal() {
      return false;
   }

   public String toString() {
      return "RemoteIndexingBackend(" + masterAddress + ")";
   }

}
