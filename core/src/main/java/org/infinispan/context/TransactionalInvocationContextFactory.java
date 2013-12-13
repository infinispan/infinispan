package org.infinispan.context;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.List;

/**
 * Invocation context to be used for transactional caches.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionalInvocationContextFactory extends AbstractInvocationContextFactory {

   private TransactionManager tm;
   private TransactionTable transactionTable;
   private BatchContainer batchContainer;
   private boolean batchingEnabled;

   @Inject
   public void init(TransactionManager tm,
         TransactionTable transactionTable, Configuration config, BatchContainer batchContainer) {
      super.init(config);
      this.tm = tm;
      this.transactionTable = transactionTable;
      this.batchContainer = batchContainer;
      this.batchingEnabled = config.invocationBatching().enabled();
   }

   @Override
   public NonTxInvocationContext createNonTxInvocationContext() {
      return newNonTxInvocationContext(true);
   }

   @Override
   public InvocationContext createSingleKeyNonTxInvocationContext() {
      return new SingleKeyNonTxInvocationContext(true, keyEq);
   }

   @Override
   public InvocationContext createInvocationContext(
         boolean isWrite, int keyCount) {
      final Transaction runningTx = getRunningTx();
      if (runningTx == null && !isWrite) {
         if (keyCount == 1)
            return createSingleKeyNonTxInvocationContext();
         else
            return newNonTxInvocationContext(true);
      }
      return createInvocationContext(runningTx);
   }

   @Override
   public InvocationContext createInvocationContext(Transaction tx) {
      if (tx == null) throw new IllegalArgumentException("Cannot create a transactional context without a valid Transaction instance.");
      LocalTxInvocationContext localContext = new LocalTxInvocationContext(keyEq);
      LocalTransaction localTransaction = transactionTable.getLocalTransaction(tx);
      localContext.setLocalTransaction(localTransaction);
      localContext.setTransaction(tx);
      return localContext;
   }

   @Override
   public LocalTxInvocationContext createTxInvocationContext() {
      return new LocalTxInvocationContext(keyEq);
   }

   @Override
   public RemoteTxInvocationContext createRemoteTxInvocationContext(
         RemoteTransaction tx, Address origin) {
      RemoteTxInvocationContext ctx = new RemoteTxInvocationContext();
      ctx.setOrigin(origin);
      ctx.setRemoteTransaction(tx);
      return ctx;
   }

   @Override
   public NonTxInvocationContext createRemoteInvocationContext(Address origin) {
      final NonTxInvocationContext nonTxInvocationContext = newNonTxInvocationContext(false);
      nonTxInvocationContext.setOrigin(origin);
      return nonTxInvocationContext;
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

   protected final NonTxInvocationContext newNonTxInvocationContext(boolean local) {
      NonTxInvocationContext ctx = new NonTxInvocationContext(keyEq);
      ctx.setOriginLocal(local);
      return ctx;
   }
}