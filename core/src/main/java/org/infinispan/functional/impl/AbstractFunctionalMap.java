package org.infinispan.functional.impl;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.api.functional.Status;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

/**
 * Abstract functional map, providing implementations for some of the shared methods.
 *
 * @since 8.0
 */
abstract class AbstractFunctionalMap<K, V> implements FunctionalMap<K, V> {
   private static final Log log = LogFactory.getLog(FunctionalMap.class);

   protected final FunctionalMapImpl<K, V> fmap;
   private final boolean transactional;
   private final boolean autoCommit;
   private final BatchContainer batchContainer;
   private final TransactionManager transactionManager;

   protected AbstractFunctionalMap(FunctionalMapImpl<K, V> fmap) {
      this.fmap = fmap;
      Configuration config = fmap.cache.getCacheConfiguration();
      transactional = config.transaction().transactionMode().isTransactional();
      autoCommit = config.transaction().autoCommit();
      transactionManager = transactional ? fmap.cache.getTransactionManager() : null;
      batchContainer = transactional && config.invocationBatching().enabled() ? fmap.cache.getBatchContainer() : null;
   }

   @Override
   public String getName() {
      return "";
   }

   @Override
   public Status getStatus() {
      return fmap.getStatus();
   }

   @Override
   public void close() throws Exception {
      fmap.close();
   }

   protected InvocationContext getInvocationContext(boolean isWrite, int keyCount) {
      InvocationContext invocationContext;
      boolean txInjected = false;
      if (transactional) {
         Transaction transaction = getOngoingTransaction();
         if (transaction == null && autoCommit && transactionManager != null) {
            try {
               transactionManager.begin();
            } catch (RuntimeException e) {
               throw e;
            } catch (Exception e) {
               throw new CacheException("Unable to begin implicit transaction.", e);
            }
            transaction = getOngoingTransaction();
            txInjected = true;
         }
         invocationContext = fmap.invCtxFactory().createInvocationContext(transaction, txInjected);
      } else {
         invocationContext = fmap.invCtxFactory().createInvocationContext(isWrite, keyCount);
      }
      return invocationContext;
   }

   protected void commitIfNeeded(InvocationContext ctx) {
      if (ctx.isInTxScope() && ((TxInvocationContext) ctx).isImplicitTransaction()) {
         if (transactionManager != null) {
            try {
               transactionManager.commit();
            } catch (Throwable e) {
               log.couldNotCompleteInjectedTransaction(e);
               throw new CacheException("Could not commit implicit transaction", e);
            }
         }
      }
   }

   protected void rollbackIfNeeded(InvocationContext ctx) {
      if (ctx.isInTxScope() && ((TxInvocationContext) ctx).isImplicitTransaction()) {
         try {
            if (transactionManager != null) transactionManager.rollback();
         } catch (Throwable t) {
            log.trace("Could not rollback", t);//best effort
         }
      }
   }

   private Transaction getOngoingTransaction() {
      try {
         Transaction transaction = null;
         if (transactionManager != null) {
            transaction = transactionManager.getTransaction();
            if (transaction == null && batchContainer != null) {
               transaction = batchContainer.getBatchTransaction();
            }
         }
         return transaction;
      } catch (SystemException e) {
         throw new CacheException("Unable to get transaction", e);
      }
   }

   protected Object invoke(InvocationContext ctx, VisitableCommand cmd) {
      Object result;
      try {
         result = fmap.chain().invoke(ctx, cmd);
      } catch (Throwable t) {
         try {
            rollbackIfNeeded(ctx);
         } catch (Throwable t2) {
            t2.addSuppressed(t);
            throw t2;
         }
         throw t;
      }
      commitIfNeeded(ctx);
      return result;
   }
}
