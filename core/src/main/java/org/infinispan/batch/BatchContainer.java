/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.batch;

import org.infinispan.CacheException;
import org.infinispan.factories.annotations.Inject;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * A container for holding thread locals for batching, to be used with the {@link org.infinispan.Cache#startBatch()} and
 * {@link org.infinispan.Cache#endBatch(boolean)} calls.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class BatchContainer {
   TransactionManager transactionManager;
   private BatchDetailsTl batchDetailsTl = new BatchDetailsTl();

   private static class BatchDetailsTl extends ThreadLocal<BatchDetails> {
      @Override
      protected BatchDetails initialValue() {
         return new BatchDetails();
      }
   }

   @Inject
   void inject(TransactionManager transactionManager) {
      this.transactionManager = transactionManager;
   }

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

            return true;
         } else {
            bd.nestedInvocationCount++;
            return false;
         }         
      }
      catch (Exception e) {
         throw new CacheException("Unable to start batch", e);
      }
      finally {
         batchDetailsTl.set(bd);
      }
   }

   public void endBatch(boolean success) {
      endBatch(false, success);
   }

   public void endBatch(boolean autoBatch, boolean success) {
      BatchDetails bd = batchDetailsTl.get();
      if (bd.tx == null) return;
      if (autoBatch) bd.nestedInvocationCount--;
      if (!autoBatch || bd.nestedInvocationCount == 0) {
         Transaction existingTx = null;
         try {
            existingTx = transactionManager.getTransaction();

            if ((existingTx == null && !autoBatch) || !bd.tx.equals(existingTx))
               transactionManager.resume(bd.tx);

            resolveTransaction(bd, success);
         }
         catch (Exception e) {
            throw new CacheException("Unable to end batch", e);
         }
         finally {
            batchDetailsTl.remove();
            try {
               if (!autoBatch && existingTx != null) transactionManager.resume(existingTx);
            }
            catch (Exception e) {
               throw new CacheException("Failed resuming existing transaction " + existingTx, e);
            }
         }
      } else {
         batchDetailsTl.set(bd);
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
      return batchDetailsTl.get().tx;
   }

   public boolean isSuspendTxAfterInvocation() {
      return batchDetailsTl.get().suspendTxAfterInvocation;
   }

   private static class BatchDetails {
      int nestedInvocationCount;
      boolean suspendTxAfterInvocation;
      Transaction tx;
      Thread thread;
   }
}
