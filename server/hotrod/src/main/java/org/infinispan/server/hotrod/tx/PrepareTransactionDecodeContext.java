package org.infinispan.server.hotrod.tx;

import java.util.List;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.cache.impl.DecoratedCache;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;

/**
 * A decode context to handle prepare request from a client.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class PrepareTransactionDecodeContext extends TransactionDecodeContext {

   private final TransactionFactory transactionFactory;
   private EmbeddedTransaction tx;
   private LocalTxInvocationContext localTxInvocationContext;

   public PrepareTransactionDecodeContext(AdvancedCache<byte[], byte[]> cache, XidImpl xid) {
      super(cache, xid);
      transactionFactory = cache.getComponentRegistry().getComponent(TransactionFactory.class);
   }

   /**
    * Starts a transaction.
    *
    * @return {@code true} if the transaction can be started, {@code false} otherwise.
    */
   public boolean startTransaction() {

      EmbeddedTransaction tx = new EmbeddedTransaction(EmbeddedTransactionManager.getInstance());
      tx.setXid(xid);
      LocalTransaction localTransaction = transactionTable
            .getOrCreateLocalTransaction(tx, false, this::newGlobalTransaction);
      TxState txState = new TxState(localTransaction.getGlobalTransaction());
      if (!serverTransactionTable.addGlobalState(xid, txState)) {
         //no need to rollback. nothing is enlisted in the transaction.
         transactionTable.removeLocalTransaction(localTransaction);
         return false;
      } else {
         this.txState = txState;
         this.tx = tx;
         this.localTxInvocationContext = new LocalTxInvocationContext(localTransaction);
         serverTransactionTable.createLocalTx(xid, tx);
         transactionTable.enlist(tx, localTransaction);
         return true;
      }
   }

   /**
    * Rollbacks the transaction if it fails to prepare.
    */
   public int rollback() {
      advance(txState.rollback());
      try {
         tx.runCommit(true);
      } catch (HeuristicMixedException e) {
         return XAException.XA_HEURMIX;
      } catch (HeuristicRollbackException e) {
         return XAException.XA_HEURRB;
      } catch (RollbackException e) {
         return XAException.XA_RBROLLBACK;
      }
      return XAException.XA_RBROLLBACK;
   }

   /**
    * Prepares the transaction.
    *
    * @param onePhaseCommit {@code true} if one phase commit.
    * @return the {@link javax.transaction.xa.XAResource#XA_OK} if successful prepared, otherwise one of the {@link
    * javax.transaction.xa.XAException} error codes.
    */
   public int prepare(boolean onePhaseCommit) {
      if (tx.runPrepare()) {
         prepared();
         if (onePhaseCommit) {
            return commit();
         }
         return XAResource.XA_OK;
      } else {
         return rollback();
      }
   }

   public void setRollbackOnly() {
      try {
         tx.setRollbackOnly();
      } catch (SystemException e) {
         //our tx implementation doesn't throw SystemException
         throw new IllegalStateException(e);
      }
   }

   public <K, V> AdvancedCache<K, V> decorateCache(AdvancedCache<K, V> cache) {
      if (cache instanceof EncoderCache && ((EncoderCache) cache).getDelegate() instanceof CacheImpl) {
         return decorateEncoderCache((EncoderCache<K, V>) cache);
      } else if (cache instanceof CacheImpl) {
         return withTransaction((CacheImpl<K, V>) cache);
      } else {
         throw new IllegalArgumentException("Unable to decorate cache");
      }
   }

   private <K, V> AdvancedCache<K, V> decorateEncoderCache(EncoderCache<K, V> cache) {
      AdvancedCache<K, V> txCache = withTransaction((CacheImpl<K, V>) cache.getDelegate());
      return cache.withCache(txCache);
   }

   private <K, V> AdvancedCache<K, V> withTransaction(CacheImpl<K, V> cache) {
      return new DecoratedCache<K, V>(cache, Flag.FORCE_WRITE_LOCK) {
         @Override
         protected InvocationContext readContext(int size) {
            return localTxInvocationContext;
         }

         @Override
         protected InvocationContext writeContext(int size) {
            return localTxInvocationContext;
         }
      };
   }

   private int commit() {
      advance(txState.commit());
      try {
         tx.runCommit(false);
      } catch (HeuristicMixedException e) {
         return XAException.XA_HEURMIX;
      } catch (HeuristicRollbackException e) {
         return XAException.XA_HEURRB;
      } catch (RollbackException e) {
         return XAException.XA_RBROLLBACK;
      }
      return XAResource.XA_OK;
   }

   /**
    * Advances the transaction to prepared state.
    */
   private void prepared() {
      List<WriteCommand> modifications = transactionTable.getLocalTransaction(tx).getModifications();
      advance(txState.prepare(modifications));
   }

   private GlobalTransaction newGlobalTransaction() {
      return transactionFactory.newGlobalTransaction(serverTransactionTable.getClientAddress(), false);
   }
}
