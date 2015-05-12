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
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

/**
 * @author bela
 * @since 4.0
 */
public class DummyTransaction implements Transaction {
   /*
    * Developer notes:
    *
    * => prepare() XA_RDONLY: the resource is finished and we shouldn't invoke the commit() or rollback().
    * => prepare() XA_RB*: the resource is rolled back and we shouldn't invoke the rollback().
    * //note// for both cases above, the commit() or rollback() will return XA_NOTA that we will ignore.
    *
    * => 1PC optimization: if we have a single XaResource, we can skip the prepare() and invoke commit(1PC).
    * => Last Resource Commit optimization: if all the resources (except last) vote XA_OK or XA_RDONLY, we can skip the
    * prepare() on the last resource and invoke commit(1PC).
    * //note// both optimization not supported since we split the commit in 2 phases for debugging
    */

   private static final Log log = LogFactory.getLog(DummyTransaction.class);
   private static boolean trace = log.isTraceEnabled();
   private final Xid xid;
   private volatile int status = Status.STATUS_UNKNOWN;
   private final List<Synchronization> syncs;
   private final List<XAResource> resources;
   private RollbackException firstRollbackException;

   public DummyTransaction(DummyBaseTransactionManager tm) {
      status = Status.STATUS_ACTIVE;
      if (tm.isUseXaXid()) {
         xid = new DummyXid(tm.transactionManagerId);
      } else {
         xid = new DummyNoXaXid();
      }
      if (trace) {
         log.tracef("Created new transaction with Xid=%s", xid);
      }
      syncs = new ArrayList<>(2);
      resources = new ArrayList<>(2);
   }

   private static boolean isRollbackCode(XAException ex) {
      /*
      XA_RBBASE      the inclusive lower bound of the rollback codes
      XA_RBROLLBACK  the rollback was caused by an unspecified reason
      XA_RBCOMMFAIL  the rollback was caused by a communication failure
      XA_RBDEADLOCK  a deadlock was detected
      XA_RBINTEGRITY a condition that violates the integrity of the resources was detected
      XA_RBOTHER     the resource manager rolled back the transaction branch for a reason not on this list
      XA_RBPROTO     a protocol error occurred in the resource manager
      XA_RBTIMEOUT   a transaction branch took too long
      XA_RBTRANSIENT may retry the transaction branch
      XA_RBEND       the inclusive upper bound of the rollback codes
       */
      return ex.errorCode >= XAException.XA_RBBASE && ex.errorCode <= XAException.XA_RBEND;
   }

   private static RollbackException newRollbackException(String message, Throwable cause) {
      RollbackException exception = new RollbackException(message);
      exception.initCause(cause);
      return exception;
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
      if (trace) {
         log.tracef("Transaction.commit() invoked in transaction with Xid=%s", xid);
      }
      checkDone("Cannot commit transaction.");
      runPrepare();
      runCommit(false);
      throwRollbackExceptionIfAny();
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
      if (trace) {
         log.tracef("Transaction.rollback() invoked in transaction with Xid=%s", xid);
      }
      checkDone("Cannot rollback transaction");
      try {
         status = Status.STATUS_MARKED_ROLLBACK;
         endResources();
         runCommit(false);
      } catch (HeuristicMixedException | HeuristicRollbackException e) {
         log.errorRollingBack(e);
         SystemException systemException = new SystemException("Unable to rollback transaction");
         systemException.initCause(e);
         throw systemException;
      }
   }

   /**
    * Mark the transaction so that the only possible outcome is a rollback.
    *
    * @throws IllegalStateException If the transaction is not in an active state.
    * @throws SystemException       If the transaction service fails in an unexpected way.
    */
   @Override
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      if (trace) {
         log.tracef("Transaction.setRollbackOnly() invoked in transaction with Xid=%s", xid);
      }
      checkDone("Cannot change status");
      markRollbackOnly(new RollbackException("Transaction marked as rollback only."));
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
   public boolean enlistResource(XAResource resource) throws RollbackException, IllegalStateException, SystemException {
      if (trace) {
         log.tracef("Transaction.enlistResource(%s) invoked in transaction with Xid=%s", resource, xid);
      }
      checkStatusBeforeRegister("resource");

      //avoid duplicates
      for (XAResource otherResource : resources) {
         try {
            if (otherResource.isSameRM(resource)) {
               log.debug("Ignoring resource. It is already there.");
               return true;
            }
         } catch (XAException e) {
            //ignored
         }
      }

      resources.add(resource);

      try {
         if (trace) {
            log.tracef("XaResource.start() invoked in transaction with Xid=%s", xid);
         }
         resource.start(xid, XAResource.TMNOFLAGS);
      } catch (XAException e) {
         if (isRollbackCode(e)) {
            RollbackException exception = newRollbackException(format("Resource %s rolled back the transaction while XaResource.start()", resource), e);
            markRollbackOnly(exception);
            log.errorEnlistingResource(e);
            throw exception;
         }
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
      if (trace) {
         log.tracef("Transaction.registerSynchronization(%s) invoked in transaction with Xid=%s", sync, xid);
      }
      checkStatusBeforeRegister("synchronization");
      if (trace) {
         log.tracef("Registering synchronization handler %s", sync);
      }
      syncs.add(sync);
   }

   public Collection<XAResource> getEnlistedResources() {
      return Collections.unmodifiableList(resources);
   }

   public boolean runPrepare() {
      if (trace) {
         log.tracef("runPrepare() invoked in transaction with Xid=%s", xid);
      }
      notifyBeforeCompletion();
      endResources();

      if (status == Status.STATUS_MARKED_ROLLBACK) {
         return false;
      }

      status = Status.STATUS_PREPARING;

      for (XAResource res : getEnlistedResources()) {
         //note: it is safe to return even if we don't prepare all the resources. rollback will be invoked.
         try {
            if (trace) {
               log.tracef("XaResource.prepare() for %s", res);
            }
            //don't need to check return value. the only possible values are OK or READ_ONLY.
            res.prepare(xid);
         } catch (XAException e) {
            if (trace) {
               log.trace("The resource wants to rollback!", e);
            }
            markRollbackOnly(newRollbackException(format("XaResource.prepare() for %s wants to rollback.", res), e));
            return false;
         } catch (Throwable th) {
            markRollbackOnly(newRollbackException(format("Unexpected error in XaResource.prepare() for %s. Rollback transaction.", res), th));
            log.unexpectedErrorFromResourceManager(th);
            return false;
         }
      }
      status = Status.STATUS_PREPARED;
      return true;
   }

   public void runCommit(boolean forceRollback) throws HeuristicMixedException, HeuristicRollbackException {
      if (trace) {
         log.tracef("runCommit(forceRollback=%b) invoked in transaction with Xid=%s", forceRollback, xid);
      }
      if (forceRollback) {
         markRollbackOnly(new RollbackException("Force rollback invoked. (debug mode)"));
      }

      int notifyAfterStatus = 0;

      try {
         if (status == Status.STATUS_MARKED_ROLLBACK) {
            notifyAfterStatus = Status.STATUS_ROLLEDBACK;
            rollbackResources();
         } else {
            notifyAfterStatus = Status.STATUS_COMMITTED;
            commitResources();
         }
      } finally {
         notifyAfterCompletion(notifyAfterStatus);
         DummyBaseTransactionManager.setTransaction(null);
      }
   }

   @Override
   public String toString() {
      return "DummyTransaction{" +
            "xid=" + xid +
            ", status=" + status +
            '}';
   }

   public XAResource firstEnlistedResource() {
      return getEnlistedResources().iterator().next();
   }

   public Xid getXid() {
      return xid;
   }

   public Collection<Synchronization> getEnlistedSynchronization() {
      return Collections.unmodifiableList(syncs);
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

   public final void throwRollbackExceptionIfAny() throws RollbackException {
      if (firstRollbackException != null) {
         throw firstRollbackException;
      }
   }

   private void markRollbackOnly(RollbackException e) {
      if (status == Status.STATUS_MARKED_ROLLBACK) {
         return;
      }
      status = Status.STATUS_MARKED_ROLLBACK;
      if (firstRollbackException == null) {
         firstRollbackException = e;
      }
   }

   private void finishResource(boolean commit) throws HeuristicRollbackException, HeuristicMixedException {
      boolean ok = false;
      boolean heuristic = false;
      boolean error = false;
      Exception cause = null;

      for (XAResource res : getEnlistedResources()) {
         try {
            if (commit) {
               if (trace) {
                  log.tracef("XaResource.commit() for %s", res);
               }
               //we only do 2-phase commits
               res.commit(xid, false);
            } else {
               if (trace) {
                  log.tracef("XaResource.rollback() for %s", res);
               }
               res.rollback(xid);
            }
            ok = true;
         } catch (XAException e) {
            cause = e;
            log.errorCommittingTx(e);
            switch (e.errorCode) {
               case XAException.XA_HEURCOM:
               case XAException.XA_HEURRB:
               case XAException.XA_HEURMIX:
                  heuristic = true;
                  break;
               case XAException.XAER_NOTA:
                  //just ignore it...
                  ok = true;
                  break;
               default:
                  error = true;
                  break;
            }
         }
      }

      resources.clear();
      if (heuristic && !ok && !error) {
         //all the resources thrown an heuristic exception
         HeuristicRollbackException exception = new HeuristicRollbackException();
         exception.initCause(cause);
         throw exception;
      } else if (error || heuristic) {
         status = Status.STATUS_UNKNOWN;
         //some resources commits, other rollbacks and others we don't know...
         HeuristicMixedException exception = new HeuristicMixedException();
         exception.initCause(cause);
         throw exception;
      }
   }

   private void commitResources() throws HeuristicRollbackException, HeuristicMixedException {
      status = Status.STATUS_COMMITTING;
      try {
         finishResource(true);
      } catch (HeuristicRollbackException | HeuristicMixedException e) {
         status = Status.STATUS_UNKNOWN;
         throw e;
      }
      status = Status.STATUS_COMMITTED;
   }

   private void rollbackResources() throws HeuristicRollbackException, HeuristicMixedException {
      status = Status.STATUS_ROLLING_BACK;
      try {
         finishResource(false);
      } catch (HeuristicRollbackException | HeuristicMixedException e) {
         status = Status.STATUS_UNKNOWN;
         throw e;
      }
      status = Status.STATUS_ROLLEDBACK;
   }

   private void notifyBeforeCompletion() {
      for (Synchronization s : getEnlistedSynchronization()) {
         if (trace) {
            log.tracef("Synchronization.beforeCompletion() for %s", s);
         }
         try {
            s.beforeCompletion();
         } catch (Throwable t) {
            markRollbackOnly(newRollbackException(format("Synchronization.beforeCompletion() for %s wants to rollback.", s), t));
            log.beforeCompletionFailed(s, t);
         }
      }
   }

   private void notifyAfterCompletion(int status) {
      for (Synchronization s : getEnlistedSynchronization()) {
         if (trace) {
            log.tracef("Synchronization.afterCompletion() for %s", s);
         }
         try {
            s.afterCompletion(status);
         } catch (Throwable t) {
            log.afterCompletionFailed(s, t);
         }
      }
      syncs.clear();
   }

   private void endResources() {
      for (XAResource resource : getEnlistedResources()) {
         if (trace) {
            log.tracef("XAResource.end() for %s", resource);
         }
         try {
            resource.end(xid, XAResource.TMSUCCESS);
         } catch (XAException e) {
            markRollbackOnly(newRollbackException(format("XaResource.end() for %s wants to rollback.", resource), e));
            log.xaResourceEndFailed(resource, e);
         } catch (Throwable t) {
            markRollbackOnly(newRollbackException(format("Unexpected error in XaResource.end() for %s. Marked as rollback", resource), t));
            log.xaResourceEndFailed(resource, t);
         }
      }
   }

   private void checkStatusBeforeRegister(String component) throws RollbackException, IllegalStateException {
      if (status == Status.STATUS_MARKED_ROLLBACK) {
         throw new RollbackException("Transaction has been marked as rollback only");
      }
      checkDone(format("Cannot register any more %s", component));
   }

   private void checkDone(String message) {
      if (isDone()) {
         throw new IllegalStateException(format("Transaction is done. %s", message));
      }
   }

   private boolean isDone() {
      switch (status) {
         case Status.STATUS_PREPARING:
         case Status.STATUS_PREPARED:
         case Status.STATUS_COMMITTING:
         case Status.STATUS_COMMITTED:
         case Status.STATUS_ROLLING_BACK:
         case Status.STATUS_ROLLEDBACK:
         case Status.STATUS_UNKNOWN:
            return true;
      }
      return false;
   }
}
