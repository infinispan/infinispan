package org.infinispan.hotrod.impl.transaction;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.transaction.operations.CompleteTransactionOperation;
import org.infinispan.hotrod.impl.transaction.operations.ForgetTransactionOperation;

/**
 * A Base {@link TransactionTable} with common logic.
 * <p>
 * It contains the functions to handle server requests that don't depend of the cache. Such operations are the commit,
 * rollback and forget request and the recovery process.
 *
 * @since 14.0
 */
abstract class AbstractTransactionTable implements TransactionTable {

   private static final Log log = LogFactory.getLog(AbstractTransactionTable.class, Log.class);

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
         throw log.transactionTableNotStarted();
      }
      return tmp;
   }

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
         return operation.execute().toCompletableFuture().get();
      } catch (Exception e) {
         log.debug("Exception while commit/rollback.", e);
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
         if (log.isTraceEnabled()) {
            log.tracef(e, "Exception in forget transaction xid=%s", xid);
         }
      }
   }

   /**
    * It requests the server for all in-doubt prepared transactions, to be handled by the recovery process.
    *
    * @return A {@link CompletionStage} which is completed with a collections of transaction {@link Xid}.
    */
   CompletionStage<Collection<Xid>> fetchPreparedTransactions() {
      try {
         TransactionOperationFactory factory = assertStartedAndReturnFactory();
         return factory.newRecoveryOperation().execute();
      } catch (Exception e) {
         if (log.isTraceEnabled()) {
            log.trace("Exception while fetching prepared transactions", e);
         }
         return CompletableFuture.completedFuture(Collections.emptyList());
      }
   }
}
