package org.infinispan.context.impl;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.CacheException;
import org.infinispan.context.AbstractInvocationContextFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;

/**
 * Invocation context to be used for transactional caches.
 *
 * @author Mircea.Markus@jboss.com
 */
@Scope(Scopes.NAMED_CACHE)
public class TransactionalInvocationContextFactory extends AbstractInvocationContextFactory {

   @Inject TransactionManager tm;
   @Inject TransactionTable transactionTable;
   @Inject BatchContainer batchContainer;

   private boolean batchingEnabled;

   @Start
   public void start() {
      this.batchingEnabled = config.invocationBatching().enabled();
   }

   @Override
   public NonTxInvocationContext createNonTxInvocationContext() {
      return newNonTxInvocationContext(null);
   }

   @Override
   public InvocationContext createSingleKeyNonTxInvocationContext() {
      return new SingleKeyNonTxInvocationContext(null);
   }

   @Override
   public InvocationContext createInvocationContext(boolean isWrite, int keyCount) {
      final Transaction runningTx = getRunningTx();
      if (runningTx == null && !isWrite) {
         if (keyCount == 1)
            return createSingleKeyNonTxInvocationContext();
         else
            return newNonTxInvocationContext(null);
      }
      return createInvocationContext(runningTx, false);
   }

   @Override
   public InvocationContext createInvocationContext(Transaction tx, boolean implicitTransaction) {
      if (tx == null) {
         throw new IllegalArgumentException("Cannot create a transactional context without a valid Transaction instance.");
      }
      LocalTransaction localTransaction = transactionTable.getOrCreateLocalTransaction(tx, implicitTransaction);
      return new LocalTxInvocationContext(localTransaction);
   }

   @Override
   public LocalTxInvocationContext createTxInvocationContext(LocalTransaction localTransaction) {
      return new LocalTxInvocationContext(localTransaction);
   }

   @Override
   public RemoteTxInvocationContext createRemoteTxInvocationContext(
         RemoteTransaction tx, Address origin) {
      RemoteTxInvocationContext ctx = new RemoteTxInvocationContext(tx);
      return ctx;
   }

   @Override
   public NonTxInvocationContext createRemoteInvocationContext(Address origin) {
      return newNonTxInvocationContext(origin);
   }

   private Transaction getRunningTx() {
      try {
         Transaction transaction = null;
         if (batchingEnabled) {
            transaction = batchContainer.getBatchTransaction();
         }
         if (transaction == null) {
            transaction = tm.getTransaction();
         }
         return transaction;
      } catch (SystemException e) {
         throw new CacheException(e);
      }
   }

   protected final NonTxInvocationContext newNonTxInvocationContext(Address origin) {
      NonTxInvocationContext ctx = new NonTxInvocationContext(origin);
      return ctx;
   }
}
