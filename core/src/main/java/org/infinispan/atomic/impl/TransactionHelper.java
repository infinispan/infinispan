package org.infinispan.atomic.impl;

import java.util.function.Supplier;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.atomic.FineGrainedAtomicMap;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.CacheException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

class TransactionHelper {
   private static final Log log = LogFactory.getLog(FineGrainedAtomicMap.class);
   private static final boolean trace = log.isTraceEnabled();

   private final TransactionManager transactionManager;
   private final BatchContainer batchContainer;
   private final boolean autoCommit;

   public TransactionHelper(AdvancedCache<?, ?> cache) {
      this.transactionManager = cache.getTransactionManager();
      if (transactionManager == null) {
         throw log.atomicFineGrainedNeedsTransactions();
      }
      this.batchContainer = cache.getCacheConfiguration().invocationBatching().enabled() ? cache.getBatchContainer() : null;
      this.autoCommit = cache.getCacheConfiguration().transaction().autoCommit();
   }

   private Transaction getOngoingTransaction() {
      try {
         Transaction transaction = transactionManager.getTransaction();
         if (transaction == null && batchContainer != null) {
            transaction = batchContainer.getBatchTransaction();
         }
         return transaction;
      } catch (SystemException e) {
         throw new CacheException("Unable to get transaction", e);
      }
   }

   <T> T run(Supplier<T> op) {
      Transaction transaction = getOngoingTransaction();
      if (transaction != null) {
         return op.get();
      }
      if (!autoCommit) {
         throw log.atomicFineGrainedNeedsExplicitTxOrAutoCommit();
      }
      try {
         transactionManager.begin();
      } catch (Throwable t) {
         throw new CacheException("Unable to begin implicit transaction.", t);
      }
      Throwable operationException = null;
      try {
         return op.get();
      } catch (Throwable t) {
         try {
            transactionManager.setRollbackOnly();
         } catch (Throwable t1) {
            t1.addSuppressed(t);
            operationException = t1;
            throw new CacheException("Unable to rollback implicit transaction.", t1);
         }
         operationException = t;
         throw t;
      } finally {
         try {
            if (transactionManager.getStatus() == Status.STATUS_ACTIVE) {
               transactionManager.commit();
            } else {
               transactionManager.rollback();
            }
         } catch (Throwable e) {
            if (operationException != null) {
               e.addSuppressed(operationException);
            }
            log.couldNotCompleteInjectedTransaction(e);
            throw new CacheException(e);
         }
      }
   }
}
