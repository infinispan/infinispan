package org.infinispan.batch;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.Inject;

/**
 * A container for holding thread locals for batching, to be used with the {@link org.infinispan.Cache#startBatch()} and
 * {@link org.infinispan.Cache#endBatch(boolean)} calls.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class BatchContainer {
   @Inject TransactionManager transactionManager;
   private final ThreadLocal<BatchDetails> batchDetailsTl = new ThreadLocal<BatchDetails>();

   /**
    * Starts a batch
    *
    * @return true if a batch was started; false if one was already available.
    */
   public boolean startBatch() throws CacheException {
      return startBatch(false);
   }

   public boolean startBatch(boolean autoBatch) throws CacheException {
      BatchDetails bd = batchDetailsTl.get();
      if (bd == null) bd = new BatchDetails();

      try {
         if (transactionManager.getTransaction() == null && bd.tx == null) {
            transactionManager.begin();
            bd.nestedInvocationCount = 1;
            bd.suspendTxAfterInvocation = !autoBatch;
            bd.thread = Thread.currentThread();

            // do not suspend if this is from an AutoBatch!
            if (autoBatch)
               bd.tx = transactionManager.getTransaction();
            else
               bd.tx = transactionManager.suspend();
            batchDetailsTl.set(bd);
            return true;
         } else {
            bd.nestedInvocationCount++;
            batchDetailsTl.set(bd);
            return false;
         }
      } catch (Exception e) {
         batchDetailsTl.remove();
         throw new CacheException("Unable to start batch", e);
      }
   }

   public void endBatch(boolean success) {
      endBatch(false, success);
   }

   public void endBatch(boolean autoBatch, boolean success) {
      BatchDetails bd = batchDetailsTl.get();
      if (bd == null) return;
      if (bd.tx == null) {
         batchDetailsTl.remove();
         return;
      }
      if (autoBatch) bd.nestedInvocationCount--;
      if (!autoBatch || bd.nestedInvocationCount == 0) {
         Transaction existingTx = null;
         try {
            existingTx = transactionManager.getTransaction();

            if ((existingTx == null && !autoBatch) || !bd.tx.equals(existingTx))
               transactionManager.resume(bd.tx);

            resolveTransaction(bd, success);
         } catch (Exception e) {
            throw new CacheException("Unable to end batch", e);
         } finally {
            batchDetailsTl.remove();
            try {
               if (!autoBatch && existingTx != null) transactionManager.resume(existingTx);
            } catch (Exception e) {
               throw new CacheException("Failed resuming existing transaction " + existingTx, e);
            }
         }
      }
   }

   private void resolveTransaction(BatchDetails bd, boolean success) throws Exception {
      Thread currentThread = Thread.currentThread();
      if (bd.thread.equals(currentThread)) {
         if (success)
            transactionManager.commit();
         else
            transactionManager.rollback();
      } else {
         if (success)
            bd.tx.commit();
         else
            bd.tx.rollback();
      }
   }

   public Transaction getBatchTransaction() {
      Transaction tx = null;
      BatchDetails bd = batchDetailsTl.get();
      if (bd != null) {
         tx = bd.tx;
         if (tx == null) batchDetailsTl.remove();
      }
      return tx;
   }

   public boolean isSuspendTxAfterInvocation() {
      BatchDetails bd = batchDetailsTl.get();
      return bd != null && bd.suspendTxAfterInvocation;
   }

   private static class BatchDetails {
      int nestedInvocationCount;
      boolean suspendTxAfterInvocation;
      Transaction tx;
      Thread thread;
   }
}
