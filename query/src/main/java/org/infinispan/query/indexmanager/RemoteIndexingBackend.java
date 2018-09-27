package org.infinispan.query.indexmanager;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hibernate.search.backend.FlushLuceneWork;
import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.PurgeAllLuceneWork;
import org.hibernate.search.indexes.serialization.spi.LuceneWorkSerializer;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.LogFactory;

/**
 * The IndexingBackend which forwards all operations to a different node
 * using Infinispan's custom commands.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2014 Red Hat Inc.
 * @since 7.0
 */
final class RemoteIndexingBackend implements IndexingBackend {

   private static final Log log = LogFactory.getLog(RemoteIndexingBackend.class, Log.class);

   private static final int GRACE_MILLISECONDS_FOR_REPLACEMENT = 4000;
   private static final int POLLING_MILLISECONDS_FOR_REPLACEMENT = 4000;

   private final String cacheName;
   private final String indexName;
   private final LuceneWorkSerializer luceneWorkSerializer;
   private final Collection<Address> recipients;
   private final RpcManager rpcManager;
   private final Address masterAddress;
   private final boolean async;

   private volatile IndexingBackend replacement;

   RemoteIndexingBackend(String cacheName, String indexName, LuceneWorkSerializer luceneWorkSerializer, RpcManager rpcManager, Address masterAddress, boolean async) {
      this.cacheName = cacheName;
      this.indexName = indexName;
      this.luceneWorkSerializer = luceneWorkSerializer;
      this.rpcManager = rpcManager;
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
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor, IndexManager indexManager) {
      try {
         sendCommand(new IndexUpdateCommand(ByteString.fromString(cacheName)), workList);
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
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor, IndexManager indexManager) {
      try {
         sendCommand(new IndexUpdateStreamCommand(ByteString.fromString(cacheName)), Collections.singletonList(singleOperation));
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

   /**
    * Decides whether commands should be sent sync or async.
    */
   private boolean shouldSendSync(List<LuceneWork> operations) {
      if (!async) {
         return true;
      }
      if (operations.size() == 1) {
         Class<? extends LuceneWork> workClass = operations.get(0).getClass(); //todo [anistor] why do we look only at the first one?

         return workClass == FlushLuceneWork.class || workClass == PurgeAllLuceneWork.class;
      }
      return false;
   }

   /**
    * Serializes the give work list using Hibernate Search's internal serializer and sets the payload to the command and
    * sends it to the master.
    *
    * @param command  an IndexUpdateCommand or IndexUpdateStreamCommand command
    * @param workList the list of LuceneWork objects to send
    */
   private void sendCommand(AbstractUpdateCommand command, List<LuceneWork> workList) {
      log.applyingChangeListRemotely(workList);

      command.setSerializedLuceneWorks(luceneWorkSerializer.toSerializedModel(workList));
      command.setIndexName(indexName);

      boolean sync = shouldSendSync(workList);
      rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(sync));

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
