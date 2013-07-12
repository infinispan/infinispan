package org.infinispan.transaction.tm;


import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author bela
 * @since 4.0
 */
public class DummyTransaction implements Transaction {
   private static final Log log = LogFactory.getLog(DummyTransaction.class);
   private static boolean trace = log.isTraceEnabled();

   private volatile int status = Status.STATUS_UNKNOWN;
   protected final DummyBaseTransactionManager tm_;
   protected final Xid xid;

   protected Set<Synchronization> syncs;
   private final List<XAResource> enlistedResources = new ArrayList<XAResource>(2);
   private int prepareStatus;

   public DummyTransaction(DummyBaseTransactionManager tm) {
      tm_ = tm;
      status = Status.STATUS_ACTIVE;
      if (tm.isUseXaXid()) {
         xid = new DummyXid(tm.transactionManagerId);
      } else {
         xid = new DummyNoXaXid();
      }
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
   @Override
   public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {

      try {
         runPrepare();
         if (status == Status.STATUS_MARKED_ROLLBACK || status == Status.STATUS_ROLLING_BACK) {
            runRollback();
            throw new RollbackException("Exception rolled back, status is: " + status);
         }
         runCommitTx();
      } finally {
         DummyBaseTransactionManager.setTransaction(null);
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
   @Override
   public void rollback() throws IllegalStateException, SystemException {
      try {
         status = Status.STATUS_ROLLING_BACK;
         runRollback();
         status = Status.STATUS_ROLLEDBACK;
         notifyAfterCompletion(Status.STATUS_ROLLEDBACK);
      } catch (Throwable t) {
         log.errorRollingBack(t);
         throw new IllegalStateException(t);
      }
      // Disassociate tx from thread.
      DummyBaseTransactionManager.setTransaction(null);
   }

   /**
    * Mark the transaction so that the only possible outcome is a rollback.
    *
    * @throws IllegalStateException If the transaction is not in an active state.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   @Override
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      status = Status.STATUS_MARKED_ROLLBACK;
   }

   /**
    * Get the status of the transaction.
    *
    * @return The status of the transaction. This is one of the {@link Status} constants.
    * @throws SystemException If the transaction service fails in an unexpected way.
    */
   @Override
   public int getStatus() throws SystemException {
      return status;
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
   @Override
   public boolean enlistResource(XAResource xaRes) throws RollbackException, IllegalStateException, SystemException {
      this.enlistedResources.add(xaRes);
      try {
         xaRes.start(xid, 0);
      } catch (XAException e) {
         log.errorEnlistingResource(e);
         throw new SystemException(e.getMessage());
      }
      return true;
   }

   /**
    * De-list an XA resource from this transaction.
    *
    * @return <code>true</code> if the resource could be de-listed from this transaction, otherwise <code>false</code>.
    * @throws IllegalStateException If the transaction is in a state where resources cannot be de-listed. This could be
    *                               because the transaction is no longer active.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   @Override
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
   @Override
   public void registerSynchronization(Synchronization sync) throws RollbackException, IllegalStateException, SystemException {
      if (sync == null)
         throw new IllegalArgumentException("null synchronization " + this);
      if (syncs == null) syncs = new HashSet<Synchronization>(8);

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

      if (trace) {
         log.tracef("registering synchronization handler %s", sync);
      }
      syncs.add(sync);

   }

   public boolean notifyBeforeCompletion() {
      boolean retval = true;
      if (syncs == null) return true;
      for (Synchronization s : syncs) {
         if (trace) log.tracef("processing beforeCompletion for %s", s);
         try {
            s.beforeCompletion();
         } catch (Throwable t) {
            retval = false;
            status = Status.STATUS_MARKED_ROLLBACK;
            log.beforeCompletionFailed(s, t);
         }
      }
      return retval;
   }

   public boolean runPrepare() throws SystemException {

      boolean successfulInit = notifyBeforeCompletion();

      if (successfulInit) {
         status = Status.STATUS_PREPARING;
      } else {
         status = Status.STATUS_ROLLING_BACK;
      }

      DummyTransaction transaction = tm_.getTransaction();
      Collection<XAResource> resources = transaction.getEnlistedResources();
      for (XAResource res : resources) {
         try {
            int prepareStatus = res.prepare(xid);
            transaction.setPrepareStatus(prepareStatus);
         } catch (XAException e) {
            log.trace("The resource wants to rollback!", e);
            status = Status.STATUS_ROLLING_BACK;
            return false;
         } catch (Throwable th) {
            log.unexpectedErrorFromResourceManager(th);
            throw new SystemException(th.getMessage());
         }
      }

      if (status == Status.STATUS_MARKED_ROLLBACK || status == Status.STATUS_ROLLING_BACK) {
         return false;
      }

      status = Status.STATUS_PREPARED;
      return true;
   }

   private void setPrepareStatus(int prepareStatus) {
      this.prepareStatus = prepareStatus;
   }

   public void notifyAfterCompletion(int status) {
      if (syncs == null) return;
      for (Synchronization s : syncs) {
         if (trace) {
            log.tracef("processing afterCompletion for %s", s);
         }
         try {
            s.afterCompletion(status);
         } catch (Throwable t) {
            log.afterCompletionFailed(s, t);
         }
      }
      syncs.clear();
   }

   public Collection<XAResource> getEnlistedResources() {
      return enlistedResources;
   }

   public void runRollback() {
      DummyTransaction transaction = tm_.getTransaction();
      Collection<XAResource> resources = transaction.getEnlistedResources();
      for (XAResource res : resources) {
         try {
            res.rollback(xid);
         } catch (XAException e) {
            log.errorRollingBack(e);
         }
      }
   }

   public void runCommitTx() throws HeuristicMixedException {
      try {
         status = Status.STATUS_COMMITTING;

         DummyTransaction transaction = tm_.getTransaction();
         if (transaction.getPrepareStatus() == XAResource.XA_RDONLY) {
            log.debug("This is a read-only tx");
         } else {
            Collection<XAResource> resources = transaction.getEnlistedResources();
            for (XAResource res : resources) {
               try {
                  //we only do 2-phase commits
                  res.commit(xid, false);
               } catch (XAException e) {
                  log.errorCommittingTx(e);
                  throw new HeuristicMixedException(e.getMessage());
               }
            }
         }
         status = Status.STATUS_COMMITTED;
      } catch (HeuristicMixedException e) {
         status = Status.STATUS_UNKNOWN;
         throw e;
      } finally {
         //notify synchronizations
         notifyAfterCompletion(status);
         DummyBaseTransactionManager.setTransaction(null);
      }
   }

   public void setStatus(int stat) {
      this.status = stat;
   }

   @Override
   public String toString() {
      return "DummyTransaction{" +
            "xid=" + xid +
            ", status=" + status +
            '}';
   }

   public int getPrepareStatus() {
      return prepareStatus;
   }

   public XAResource firstEnlistedResource() {
      return getEnlistedResources().iterator().next();
   }

   public Xid getXid() {
      return xid;
   }

   public Collection<Synchronization> getEnlistedSynchronization() {
      return syncs;
   }

   /**
    * Must be defined for increased performance
    */
   @Override
   public final int hashCode() {
      return xid.hashCode();
   }

   @Override
   public final boolean equals(Object obj) {
      return this == obj;
   }

}
