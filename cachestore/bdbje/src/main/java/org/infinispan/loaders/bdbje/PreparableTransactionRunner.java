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
package org.infinispan.loaders.bdbje;

import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.compat.DbCompat;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.util.ExceptionUnwrapper;

/**
 * Adapted version of {@link TransactionRunner}, which allows us to prepare a transaction without committing it.<p/> The
 * transaction prepared is accessible via {@link com.sleepycat.collections.CurrentTransaction#getTransaction()}
 *
 * @author Adrian Cole
 * @since 4.0
 */
public class PreparableTransactionRunner extends TransactionRunner {
   CurrentTransaction currentTxn;

   /**
    * Delegates to the {@link  TransactionRunner#TransactionRunner(com.sleepycat.je.Environment, int,
    * com.sleepycat.je.TransactionConfig) superclass} and caches a current reference to {@link CurrentTransaction}.
    *
    * @see TransactionRunner#TransactionRunner(com.sleepycat.je.Environment, int, com.sleepycat.je.TransactionConfig)
    */
   public PreparableTransactionRunner(Environment env, int maxRetries, TransactionConfig config) {
      super(env, maxRetries, config);
      this.currentTxn = CurrentTransaction.getInstance(env);
   }

   /**
    * Delegates to the {@link  TransactionRunner#TransactionRunner(com.sleepycat.je.Environment) superclass} and caches
    * a current reference to {@link CurrentTransaction}.
    *
    * @see TransactionRunner#TransactionRunner(com.sleepycat.je.Environment)
    */
   public PreparableTransactionRunner(Environment env) {
      super(env);
      this.currentTxn = CurrentTransaction.getInstance(env);
   }

   /**
    * Same behaviour as {@link TransactionRunner#run(com.sleepycat.collections.TransactionWorker) run}, except that the
    * transaction is not committed on success.
    *
    * @see TransactionRunner#run(com.sleepycat.collections.TransactionWorker)
    */
   public void prepare(TransactionWorker worker) throws Exception {
      for (int currentTries = 0; ; currentTries++) {
         Transaction txn = null;
         try {
            txn = currentTxn.beginTransaction(getTransactionConfig());
            worker.doWork();
            return;
         } catch (Throwable caught) {
            currentTries = abortOverflowingCurrentTriesOnError(txn, currentTries);
            caught = ExceptionUnwrapper.unwrapAny(caught);
            rethrowIfNotDeadLock(caught);
            if (currentTries >= getMaxRetries()) throw (LockConflictException) caught;
         }
      }
   }

   int abortOverflowingCurrentTriesOnError(Transaction toAbort, int currentTries) {
      if (toAbort != null && toAbort == currentTxn.getTransaction()) {
         try {
            currentTxn.abortTransaction();
         } catch (Throwable problemAborting) {
            /* superclass prints to stderr, so we will also */
            if (DbCompat.TRANSACTION_RUNNER_PRINT_STACK_TRACES) {
               problemAborting.printStackTrace();
            }
            /* Force the original exception to be thrown. */
            return Integer.MAX_VALUE;
         }
      }
      return currentTries;
   }

   void rethrowIfNotDeadLock(Throwable caught) throws Exception {
      if (!(caught instanceof LockConflictException)) {
         if (caught instanceof Exception) {
            throw (Exception) caught;
         } else {
            throw (Error) caught;
         }
      }
   }

}
