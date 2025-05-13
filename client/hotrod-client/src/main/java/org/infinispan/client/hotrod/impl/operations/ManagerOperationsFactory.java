package org.infinispan.client.hotrod.impl.operations;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.operations.CompleteTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.operations.ForgetTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.operations.RecoveryOperation;

public class ManagerOperationsFactory {

   private static final ManagerOperationsFactory INSTANCE = new ManagerOperationsFactory();

   private ManagerOperationsFactory() {

   }

   public static ManagerOperationsFactory getInstance() {
      return INSTANCE;
   }

   /**
    * A ping operation for a specific cache to ensure it is running and receive its configuration details. This command
    * will be retried on another server if it encounters connectivity issues.
    * @param cacheName
    * @return
    */
   public HotRodOperation<PingResponse> newPingOperation(String cacheName) {
      return new CachePingOperation(cacheName);
   }

   public HotRodOperation<String> executeOperation(String taskName, Map<String, byte[]> marshalledParams) {
      return new NoCacheExecuteOperation(taskName, marshalledParams);
   }

   public HotRodOperation<Integer> newCompleteTransactionOperation(Xid xid, boolean commit) {
      return new CompleteTransactionOperation(xid, commit);
   }

   public HotRodOperation<Void> newForgetTransactionOperation(Xid xid) {
      return new ForgetTransactionOperation(xid);
   }

   public HotRodOperation<Collection<Xid>> newRecoveryOperation() {
      return new RecoveryOperation();
   }

   public HotRodOperation<Integer> newPrepareTransaction(String cacheName, Xid xid, boolean onePhaseCommit,
                                                         List<Modification> modifications,
                                                         boolean recoverable, long timeoutMs) {
      return new NoCachePrepareTransactionOperation(cacheName, xid, onePhaseCommit, modifications, recoverable, timeoutMs);
   }
}
