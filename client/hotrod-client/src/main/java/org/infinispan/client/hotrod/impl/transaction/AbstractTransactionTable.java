package org.infinispan.client.hotrod.impl.transaction;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.impl.transaction.operations.CompleteTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.operations.ForgetTransactionOperation;
import org.infinispan.client.hotrod.logging.Log;

/**
 * A Base {@link TransactionTable} with common logic.
 * <p>
 * It contains the functions to handle server requests that don't depend of the cache. Such operations are the commit,
 * rollback and forget request and the recovery process.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
abstract class AbstractTransactionTable implements TransactionTable {

   private final long timeout;
   private volatile TransactionOperationFactory operationFactory;

   AbstractTransactionTable(long timeout) {
      this.timeout = timeout;
   }

   @Override
   public final void start(TransactionOperationFactory operationFactory) {
      this.operationFactory = operationFactory;
   }

   /**
    * Check this component has started (i.e. {@link TransactionOperationFactory} isn't null)
    *
    * @return the {@link TransactionOperationFactory} to use.
    */
   TransactionOperationFactory assertStartedAndReturnFactory() {
      TransactionOperationFactory tmp = operationFactory;
      if (tmp == null) {
         throw getLog().transactionTableNotStarted();
      }
      return tmp;
   }

   abstract Log getLog();

   abstract boolean isTraceLogEnabled();

   final long getTimeout() {
      return timeout;
   }

   /**
    * It completes the transaction with the commit or rollback request.
    * <p>
    * It can be a commit or rollback request.
    *
    * @param xid    The transaction {@link Xid}.
    * @param commit {@code True} to commit the transaction, {@link false} to rollback.
    * @return The server's return code.
    */
   int completeTransaction(Xid xid, boolean commit) {
      try {
         TransactionOperationFactory factory = assertStartedAndReturnFactory();
         CompleteTransactionOperation operation = factory.newCompleteTransactionOperation(xid, commit);
         return operation.execute().get();
      } catch (Exception e) {
         getLog().debug("Exception while commit/rollback.", e);
         return XAException.XA_HEURRB; //heuristically rolled-back
      }
   }

   /**
    * Tells the server to forget this transaction.
    *
    * @param xid The transaction {@link Xid}.
    */
   void forgetTransaction(Xid xid) {
      try {
         TransactionOperationFactory factory = assertStartedAndReturnFactory();
         ForgetTransactionOperation operation = factory.newForgetTransactionOperation(xid);
         //async.
         //we don't need the reply from server. If we can't forget for some reason (timeouts or other exception),
         // the server reaper will cleanup the completed transactions after a while. (default 1 min)
         operation.execute();
      } catch (Exception e) {
         if (isTraceLogEnabled()) {
            getLog().tracef(e, "Exception in forget transaction xid=%s", xid);
         }
      }
   }

   /**
    * It requests the server for all in-doubt prepared transactions, to be handled by the recovery process.
    *
    * @return A {@link CompletableFuture} which is completed with a collections of transaction {@link Xid}.
    */
   CompletableFuture<Collection<Xid>> fetchPreparedTransactions() {
      try {
         TransactionOperationFactory factory = assertStartedAndReturnFactory();
         return factory.newRecoveryOperation().execute();
      } catch (Exception e) {
         if (isTraceLogEnabled()) {
            getLog().trace("Exception while fetching prepared transactions", e);
         }
         return CompletableFuture.completedFuture(Collections.emptyList());
      }
   }
}
