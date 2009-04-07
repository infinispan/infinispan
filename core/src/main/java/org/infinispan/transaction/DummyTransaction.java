/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.transaction;

import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author bela
 * @since 4.0
 */
public class DummyTransaction implements Transaction {
   private int status = Status.STATUS_UNKNOWN;
   private static final Log log = LogFactory.getLog(DummyTransaction.class);
   protected DummyBaseTransactionManager tm_;

   protected final Set<Synchronization> participants = new CopyOnWriteArraySet<Synchronization>();

   public DummyTransaction(DummyBaseTransactionManager tm) {
      tm_ = tm;
      status = Status.STATUS_ACTIVE;
   }

   /**
    * Attempt to commit this transaction.
    *
    * @throws RollbackException          If the transaction was marked for rollback only, the transaction is rolled back
    *                                    and this exception is thrown.
    * @throws SystemException            If the transaction service fails in an unexpected way.
    * @throws HeuristicMixedException    If a heuristic decision was made and some some parts of the transaction have
    *                                    been committed while other parts have been rolled back.
    * @throws HeuristicRollbackException If a heuristic decision to roll back the transaction was made.
    * @throws SecurityException          If the caller is not allowed to commit this transaction.
    */
   public void commit()
         throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
                SecurityException, SystemException {
      boolean doCommit;
      status = Status.STATUS_PREPARING;
      try {
         boolean outcome = notifyBeforeCompletion();
         // status=Status.STATUS_PREPARED;
         if (outcome && status != Status.STATUS_MARKED_ROLLBACK) {
            status = Status.STATUS_COMMITTING;
            doCommit = true;
         } else {
            status = Status.STATUS_ROLLING_BACK;
            doCommit = false;
         }
         notifyAfterCompletion(doCommit ? Status.STATUS_COMMITTED : Status.STATUS_MARKED_ROLLBACK);
         status = doCommit ? Status.STATUS_COMMITTED : Status.STATUS_MARKED_ROLLBACK;
         if (!doCommit) throw new RollbackException("outcome is " + outcome + " status: " + status);
      }
      finally {
         // Disassociate tx from thread.
         tm_.setTransaction(null);
      }
   }

   /**
    * Rolls back this transaction.
    *
    * @throws IllegalStateException If the transaction is in a state where it cannot be rolled back. This could be
    *                               because the transaction is no longer active, or because it is in the {@link
    *                               Status#STATUS_PREPARED prepared state}.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   public void rollback() throws IllegalStateException, SystemException {
      try {
         // JBCACHE-360 -- to match JBossTM (and presumable the spec) a
         // rollback transaction should have status ROLLEDBACK before
         // calling afterCompletion().
         //status=Status.STATUS_ROLLING_BACK;
         status = Status.STATUS_ROLLEDBACK;
         notifyAfterCompletion(Status.STATUS_ROLLEDBACK);
      }
      catch (Throwable t) {
      }
      status = Status.STATUS_ROLLEDBACK;

      // Disassociate tx from thread.
      tm_.setTransaction(null);
   }

   /**
    * Mark the transaction so that the only possible outcome is a rollback.
    *
    * @throws IllegalStateException If the transaction is not in an active state.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      status = Status.STATUS_MARKED_ROLLBACK;
   }

   /**
    * Get the status of the transaction.
    *
    * @return The status of the transaction. This is one of the {@link Status} constants.
    * @throws SystemException If the transaction service fails in an unexpected way.
    */
   public int getStatus() throws SystemException {
      return status;
   }

   /**
    * Change the transaction timeout for transactions started by the calling thread with the {@link
    * DummyTransactionManager#begin()} method.
    *
    * @param seconds The new timeout value, in seconds. If this parameter is <code>0</code>, the timeout value is reset
    *                to the default value.
    * @throws SystemException If the transaction service fails in an unexpected way.
    */
   public void setTransactionTimeout(int seconds) throws SystemException {
      throw new SystemException("not supported");
   }

   /**
    * Enlist an XA resource with this transaction.
    *
    * @return <code>true</code> if the resource could be enlisted with this transaction, otherwise <code>false</code>.
    * @throws RollbackException     If the transaction is marked for rollback only.
    * @throws IllegalStateException If the transaction is in a state where resources cannot be enlisted. This could be
    *                               because the transaction is no longer active, or because it is in the {@link
    *                               Status#STATUS_PREPARED prepared state}.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   public boolean enlistResource(XAResource xaRes)
         throws RollbackException, IllegalStateException, SystemException {
      throw new SystemException("not supported");
   }

   /**
    * Delist an XA resource from this transaction.
    *
    * @return <code>true</code> if the resource could be delisted from this transaction, otherwise <code>false</code>.
    * @throws IllegalStateException If the transaction is in a state where resources cannot be delisted. This could be
    *                               because the transaction is no longer active.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   public boolean delistResource(XAResource xaRes, int flag)
         throws IllegalStateException, SystemException {
      throw new SystemException("not supported");
   }

   /**
    * Register a {@link Synchronization} callback with this transaction.
    *
    * @throws RollbackException     If the transaction is marked for rollback only.
    * @throws IllegalStateException If the transaction is in a state where {@link Synchronization} callbacks cannot be
    *                               registered. This could be because the transaction is no longer active, or because it
    *                               is in the {@link Status#STATUS_PREPARED prepared state}.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   public void registerSynchronization(Synchronization sync)
         throws RollbackException, IllegalStateException, SystemException {
      if (sync == null)
         throw new IllegalArgumentException("null synchronization " + this);

      switch (status) {
         case Status.STATUS_ACTIVE:
         case Status.STATUS_PREPARING:
            break;
         case Status.STATUS_PREPARED:
            throw new IllegalStateException("already prepared. " + this);
         case Status.STATUS_COMMITTING:
            throw new IllegalStateException("already started committing. " + this);
         case Status.STATUS_COMMITTED:
            throw new IllegalStateException("already committed. " + this);
         case Status.STATUS_MARKED_ROLLBACK:
            throw new RollbackException("already marked for rollback " + this);
         case Status.STATUS_ROLLING_BACK:
            throw new RollbackException("already started rolling back. " + this);
         case Status.STATUS_ROLLEDBACK:
            throw new RollbackException("already rolled back. " + this);
         case Status.STATUS_NO_TRANSACTION:
            throw new IllegalStateException("no transaction. " + this);
         case Status.STATUS_UNKNOWN:
            throw new IllegalStateException("unknown state " + this);
         default:
            throw new IllegalStateException("illegal status: " + status + " tx=" + this);
      }

      if (log.isDebugEnabled()) {
         log.debug("registering synchronization handler " + sync);
      }
      participants.add(sync);

   }

   void setStatus(int new_status) {
      status = new_status;
   }

   protected boolean notifyBeforeCompletion() {
      boolean retval = true;

      for (Synchronization s : participants) {
         if (log.isDebugEnabled()) {
            log.debug("processing beforeCompletion for " + s);
         }
         try {
            s.beforeCompletion();
         }
         catch (Throwable t) {
            retval = false;
            log.error("beforeCompletion() failed for " + s, t);
         }
      }
      return retval;
   }

   protected void notifyAfterCompletion(int status) {
      for (Synchronization s : participants) {
         if (log.isDebugEnabled()) {
            log.debug("processing afterCompletion for " + s);
         }
         try {
            s.afterCompletion(status);
         }
         catch (Throwable t) {
            log.error("afterCompletion() failed for " + s, t);
         }
      }
      participants.clear();
   }

}
