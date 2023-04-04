package org.infinispan.commons.tx;


import static java.lang.String.format;
import static org.infinispan.commons.util.concurrent.CompletableFutures.asCompletionException;
import static org.infinispan.commons.util.concurrent.CompletableFutures.completedFalse;
import static org.infinispan.commons.util.concurrent.CompletableFutures.completedNull;
import static org.infinispan.commons.util.concurrent.CompletableFutures.completedTrue;
import static org.infinispan.commons.util.concurrent.CompletableFutures.extractException;
import static org.infinispan.commons.util.concurrent.CompletableFutures.identity;
import static org.infinispan.commons.util.concurrent.CompletableFutures.rethrowExceptionIfPresent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;


/**
 * A basic {@link Transaction} implementation.
 *
 * @author Bela Ban
 * @author Pedro Ruivo
 * @since 9.1
 */
public class TransactionImpl implements Transaction {
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

   private static final Log log = LogFactory.getLog(TransactionImpl.class);
   private static final String FORCE_ROLLBACK_MESSAGE = "Force rollback invoked. (debug mode)";
   private final List<Synchronization> syncs;
   private final List<XaResourceData> resources;
   private final Object xidLock = new Object();
   private volatile XidImpl xid;
   private volatile int status;
   private RollbackException firstRollbackException;

   protected TransactionImpl() {
      status = Status.STATUS_ACTIVE;
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

   private static Throwable throwChecked(Throwable throwable) throws RollbackException, HeuristicMixedException, HeuristicRollbackException {
      throwable = extractException(throwable);
      if (throwable instanceof HeuristicMixedException) {
         throw (HeuristicMixedException) throwable;
      } else if (throwable instanceof HeuristicRollbackException) {
         throw (HeuristicRollbackException) throwable;
      } else if (throwable instanceof RollbackException) {
         throw (RollbackException) throwable;
      } else if (throwable instanceof RuntimeException) {
         throw (RuntimeException) throwable;
      }
      return throwable;
   }

   private static void throwRuntimeException(Throwable throwable) {
      if (throwable instanceof RuntimeException) {
         throw (RuntimeException) throwable;
      } else {
         throw new RuntimeException(throwable);
      }
   }

   private static Void checkThrowableForRollback(Throwable t) {
      t = extractException(t);
      if (t instanceof HeuristicMixedException || t instanceof HeuristicRollbackException) {
         log.errorRollingBack(t);
         SystemException systemException = new SystemException("Unable to rollback transaction");
         systemException.initCause(t);
         throw asCompletionException(systemException);
      } else if (t instanceof RollbackException) {
         //ignored
         if (log.isTraceEnabled()) {
            log.trace("RollbackException thrown while rolling back", t);
         }
      }
      return null;
   }


   /**
    * Attempt to commit this transaction.
    *
    * @throws RollbackException          If the transaction was marked for rollback only, the transaction is rolled back
    *                                    and this exception is thrown.
    * @throws HeuristicMixedException    If a heuristic decision was made and some some parts of the transaction have
    *                                    been committed while other parts have been rolled back.
    * @throws HeuristicRollbackException If a heuristic decision to roll back the transaction was made.
    * @throws SecurityException          If the caller is not allowed to commit this transaction.
    */
   @Override
   public void commit()
         throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException {
      try {
         commitAsync(DefaultResourceConverter.INSTANCE).toCompletableFuture().get();
      } catch (ExecutionException e) {
         Throwable cause = throwChecked(e.getCause());
         if (cause instanceof SecurityException) {
            throw (SecurityException) cause;
         }
         throwRuntimeException(cause);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }

   public CompletionStage<Void> commitAsync(TransactionResourceConverter converter) {
      if (log.isTraceEnabled()) {
         log.tracef("Transaction.commit() invoked in transaction with Xid=%s", xid);
      }
      if (isDone()) {
         return CompletableFuture.failedFuture(new IllegalStateException("Transaction is done. Cannot commit transaction."));
      }
      return runPrepareAsync(converter)
            .handle((____, ___) -> runCommitAsync(false, converter))
            .thenCompose(Function.identity());
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
         rollbackAsync(DefaultResourceConverter.INSTANCE).toCompletableFuture().get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         Throwable cause = extractException(e.getCause());
         if (cause instanceof IllegalStateException) {
            throw (IllegalStateException) cause;
         } else if (cause instanceof SystemException) {
            throw (SystemException) cause;
         }
         throwRuntimeException(cause);
      }
   }

   public CompletionStage<Void> rollbackAsync(TransactionResourceConverter converter) {
      if (log.isTraceEnabled()) {
         log.tracef("Transaction.commit() invoked in transaction with Xid=%s", xid);
      }
      if (isDone()) {
         return CompletableFuture.failedFuture(new IllegalStateException("Transaction is done. Cannot commit transaction."));
      }
      status = Status.STATUS_MARKED_ROLLBACK;

      return asyncEndXaResources(converter)
            .thenCompose(unused -> runCommitAsync(false, converter))
            .exceptionally(TransactionImpl::checkThrowableForRollback);
   }

   /**
    * Mark the transaction so that the only possible outcome is a rollback.
    *
    * @throws IllegalStateException If the transaction is not in an active state.
    */
   @Override
   public void setRollbackOnly() throws IllegalStateException {
      if (log.isTraceEnabled()) {
         log.tracef("Transaction.setRollbackOnly() invoked in transaction with Xid=%s", xid);
      }
      if (isDone()) {
         throw new IllegalStateException("Transaction is done. Cannot change status");
      }
      markRollbackOnly(new RollbackException("Transaction marked as rollback only."));
   }

   /**
    * Get the status of the transaction.
    *
    * @return The status of the transaction. This is one of the {@link Status} constants.
    */
   @Override
   public int getStatus() {
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
      if (log.isTraceEnabled()) {
         log.tracef("Transaction.enlistResource(%s) invoked in transaction with Xid=%s", resource, xid);
      }
      checkStatusBeforeRegister("resource");

      //avoid duplicates
      for (XaResourceData other : resources) {
         try {
            if (other.xaResource.isSameRM(resource)) {
               log.debug("Ignoring resource. It is already there.");
               return true;
            }
         } catch (XAException e) {
            //ignored
         }
      }

      synchronized (xidLock) {
         resources.add(new XaResourceData(resource));
      }

      try {
         if (log.isTraceEnabled()) {
            log.tracef("XaResource.start() invoked in transaction with Xid=%s", xid);
         }
         resource.start(xid, XAResource.TMNOFLAGS);
      } catch (XAException e) {
         if (isRollbackCode(e)) {
            RollbackException exception = newRollbackException(
                  format("Resource %s rolled back the transaction while XaResource.start()", resource), e);
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
    */
   @Override
   public void registerSynchronization(Synchronization sync) throws RollbackException, IllegalStateException {
      if (log.isTraceEnabled()) {
         log.tracef("Transaction.registerSynchronization(%s) invoked in transaction with Xid=%s", sync, xid);
      }
      checkStatusBeforeRegister("synchronization");
      if (log.isTraceEnabled()) {
         log.tracef("Registering synchronization handler %s", sync);
      }
      synchronized (xidLock) {
         syncs.add(sync);
      }
   }

   public Collection<XAResource> getEnlistedResources() {
      return resources.stream().map(xaResourceData -> xaResourceData.xaResource).collect(Collectors.toUnmodifiableList());
   }

   public boolean runPrepare() {
      return runPrepareAsync(DefaultResourceConverter.INSTANCE).toCompletableFuture().join();
   }

   public CompletionStage<Boolean> runPrepareAsync(TransactionResourceConverter resourceConverter) {
      TransactionResourceConverter converter = resourceConverter == null ? DefaultResourceConverter.INSTANCE : resourceConverter;
      if (log.isTraceEnabled()) {
         log.tracef("asyncPrepare() invoked in transaction with Xid=%s", xid);
      }
      // notify Synchronizations
      CompletionStage<Void> cf = asyncBeforeCompletion(converter);

      // XaResource.end()
      cf = cf.thenCompose(unused -> asyncEndXaResources(converter));

      // XaResource.prepare()
      return cf.thenCompose(unused -> {
         if (status == Status.STATUS_MARKED_ROLLBACK) {
            //no need for prepare since we are going to rollback
            return completedFalse();
         }
         status = Status.STATUS_PREPARING;
         return asyncPrepareXaResources(converter);
      });
   }

   /**
    * Runs the second phase of two-phase-commit protocol.
    * <p>
    * If {@code forceRollback} is {@code true}, then a {@link RollbackException} is thrown with the message {@link
    * #FORCE_ROLLBACK_MESSAGE}.
    *
    * @param forceRollback force the transaction to rollback.
    */
   public synchronized void runCommit(boolean forceRollback) //synchronized because of client transactions
         throws HeuristicMixedException, HeuristicRollbackException, RollbackException {
      try {
         runCommitAsync(forceRollback, DefaultResourceConverter.INSTANCE).toCompletableFuture().get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         Throwable cause = throwChecked(e.getCause());
         throwRuntimeException(cause);
      }
   }

   public CompletionStage<Void> runCommitAsync(boolean forceRollback, TransactionResourceConverter resourceConverter) {
      TransactionResourceConverter converter = resourceConverter == null ? DefaultResourceConverter.INSTANCE : resourceConverter;
      if (log.isTraceEnabled()) {
         log.tracef("runCommit(forceRollback=%b) invoked in transaction with Xid=%s", forceRollback, xid);
      }
      if (forceRollback) {
         markRollbackOnly(new RollbackException(FORCE_ROLLBACK_MESSAGE));
      }

      boolean commit = status != Status.STATUS_MARKED_ROLLBACK;
      CompletionStage<Void> stage = asyncFinishXaResources(commit, converter);
      //notify Synchronizations
      stage = stage.handle((__, t) -> {
         CompletionStage<Void> s = asyncAfterCompletion(commit ? Status.STATUS_COMMITTED : Status.STATUS_ROLLEDBACK, converter);
         if (t != null) {
            return s.thenAccept(___ -> {
               throw asCompletionException(t);
            });
         } else {
            return s.thenAccept(___ -> rethrowExceptionIfPresent(hasRollbackException(forceRollback)));
         }
      }).thenCompose(identity());
      TransactionManagerImpl.dissociateTransaction();
      resources.clear();
      return stage;
   }

   @Override
   public String toString() {
      return "TransactionImpl{" +
            "xid=" + xid +
            ", status=" + Util.transactionStatusToString(status) +
            '}';
   }

   public XidImpl getXid() {
      return xid;
   }

   public void setXid(XidImpl xid) {
      synchronized (xidLock) {
         if (syncs.isEmpty() && resources.isEmpty()) {
            this.xid = xid;
         }
      }
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

   private RollbackException hasRollbackException(boolean forceRollback) {
      if (firstRollbackException != null) {
         if (forceRollback && FORCE_ROLLBACK_MESSAGE.equals(firstRollbackException.getMessage())) {
            //force rollback set. don't throw it.
            return null;
         }
         return firstRollbackException;
      }
      return null;
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

   private CompletionStage<Void> asyncBeforeCompletion(TransactionResourceConverter converter) {
      Iterator<Synchronization> iterator = syncs.iterator();
      if (!iterator.hasNext()) {
         return completedNull();
      }
      CompletionStage<Void> cf = beforeCompletion(iterator.next(), converter);
      while (iterator.hasNext()) {
         Synchronization synchronization = iterator.next();
         cf = cf.thenCompose(unused -> beforeCompletion(synchronization, converter));
      }
      return cf;
   }

   private CompletionStage<Void> beforeCompletion(Synchronization s, TransactionResourceConverter converter) {
      if (log.isTraceEnabled()) {
         log.tracef("Synchronization.beforeCompletion() for %s", s);
      }
      return converter.convertSynchronization(s).asyncBeforeCompletion().exceptionally(t -> {
         beforeCompletionFailed(s, t);
         return null;
      });
   }

   private void beforeCompletionFailed(Synchronization s, Throwable t) {
      t = extractException(t);
      markRollbackOnly(newRollbackException(format("Synchronization.beforeCompletion() for %s wants to rollback.", s), t));
      log.beforeCompletionFailed(s.toString(), t);
   }

   private CompletionStage<Void> asyncAfterCompletion(int status, TransactionResourceConverter converter) {
      Iterator<Synchronization> iterator = syncs.iterator();
      if (!iterator.hasNext()) {
         return completedNull();
      }
      CompletionStage<Void> cf = afterCompletion(iterator.next(), status, converter);
      while (iterator.hasNext()) {
         Synchronization synchronization = iterator.next();
         cf = cf.thenCompose(unused -> afterCompletion(synchronization, status, converter));
      }
      syncs.clear();
      return cf;
   }

   private static CompletionStage<Void> afterCompletion(Synchronization s, int status, TransactionResourceConverter converter) {
      if (log.isTraceEnabled()) {
         log.tracef("Synchronization.afterCompletion() for %s", s);
      }
      return converter.convertSynchronization(s).asyncAfterCompletion(status).exceptionally(t -> {
         log.afterCompletionFailed(s.toString(), extractException(t));
         return null;
      });
   }


   private CompletionStage<Void> asyncEndXaResources(TransactionResourceConverter converter) {
      Iterator<XaResourceData> iterator = resources.iterator();
      if (!iterator.hasNext()) {
         return completedNull();
      }
      CompletionStage<Void> cf = endXaResource(iterator.next().xaResource, converter);
      while (iterator.hasNext()) {
         XAResource resource = iterator.next().xaResource;
         cf = cf.thenCompose(unused -> endXaResource(resource, converter));
      }
      return cf;
   }

   private CompletionStage<Void> endXaResource(XAResource resource, TransactionResourceConverter converter) {
      if (log.isTraceEnabled()) {
         log.tracef("XAResource.end() for %s", resource);
      }
      return converter.convertXaResource(resource).asyncEnd(xid, XAResource.TMSUCCESS).exceptionally(t -> {
         endXaResourceFailed(resource, t);
         return null;
      });
   }

   private void endXaResourceFailed(XAResource resource, Throwable t) {
      t = extractException(t);
      String msg = t instanceof XAException ?
            format("XaResource.end() for %s wants to rollback.", resource) :
            format("Unexpected error in XaResource.end() for %s. Marked as rollback", resource);
      markRollbackOnly(newRollbackException(msg, t));
      log.xaResourceEndFailed(resource.toString(), t);
   }

   private CompletionStage<Boolean> asyncPrepareXaResources(TransactionResourceConverter converter) {
      status = Status.STATUS_PREPARING;
      Iterator<XaResourceData> iterator = resources.iterator();
      if (!iterator.hasNext()) {
         status = Status.STATUS_PREPARED;
         return completedTrue();
      }

      CompletionStage<Boolean> cf = prepareXaResource(iterator.next(), converter);
      while (iterator.hasNext()) {
         XaResourceData data = iterator.next();
         cf = cf.thenCompose(prepared -> prepared ? prepareXaResource(data, converter) : completedFalse());
      }
      return cf.whenComplete((prepared, ___) -> {
         if (prepared) {
            status = Status.STATUS_PREPARED;
         }
      });
   }

   private CompletionStage<Boolean> prepareXaResource(XaResourceData data, TransactionResourceConverter converter) {
      if (log.isTraceEnabled()) {
         log.tracef("XaResource.prepare() for %s", data.xaResource);
      }
      return converter.convertXaResource(data.xaResource).asyncPrepare(xid)
            .thenApply(status -> {
               data.status = status;
               return true;
            }).exceptionally(t -> {
               prepareXaResourceFailed(data.xaResource, t);
               return false;
            });
   }

   private void prepareXaResourceFailed(XAResource resource, Throwable t) {
      t = extractException(t);
      String msg;
      if (t instanceof XAException) {
         if (log.isTraceEnabled()) {
            log.tracef(t, "XaResource.prepare() for %s wants to rollback.", resource);
         }
         msg = format("XaResource.prepare() for %s wants to rollback.", resource);
      } else {
         msg = format("Unexpected error in XaResource.prepare() for %s. Rollback transaction.", resource);
         log.unexpectedErrorFromResourceManager(t);
      }
      markRollbackOnly(newRollbackException(msg, t));
   }

   private CompletionStage<Void> asyncFinishXaResources(boolean commit, TransactionResourceConverter converter) {
      Iterator<XaResourceData> iterator = resources.iterator();
      if (!iterator.hasNext()) {
         status = commit ? Status.STATUS_COMMITTED : Status.STATUS_ROLLEDBACK;
         return completedNull();
      }
      XaResultCollector collector = new XaResultCollector(resources.size(), commit);
      CompletionStage<Void> cf = commit ?
            commitXaResource(iterator.next(), converter, collector) :
            rollbackXaResource(iterator.next(), converter, collector);
      while (iterator.hasNext()) {
         XaResourceData data = iterator.next();
         cf = cf.thenCompose(unused -> commit ?
               commitXaResource(data, converter, collector) :
               rollbackXaResource(data, converter, collector));
      }
      return cf.thenApply(unused -> {
         checkCollector(collector, commit);
         return null;
      });
   }

   private CompletionStage<Void> commitXaResource(XaResourceData data, TransactionResourceConverter converter, XaResultCollector collector) {
      if (data.status == XAResource.XA_RDONLY) {
         log.tracef("Skipping XaResource.commit() since prepare status was XA_RDONLY for %s", data.xaResource);
         return completedNull();
      }
      if (log.isTraceEnabled()) {
         log.tracef("XaResource.commit() for %s", data.xaResource);
      }
      return converter.convertXaResource(data.xaResource).asyncCommit(xid, false)
            .thenRun(collector)
            .exceptionally(collector);
   }

   private CompletionStage<Void> rollbackXaResource(XaResourceData data, TransactionResourceConverter converter, XaResultCollector collector) {
      if (data.status == XAResource.XA_RDONLY) {
         log.tracef("Skipping XaResource.rollback() since prepare status was XA_RDONLY for %s", data.xaResource);
         return completedNull();
      }
      if (log.isTraceEnabled()) {
         log.tracef("XaResource.rollback() for %s", data.xaResource);
      }
      return converter.convertXaResource(data.xaResource).asyncRollback(xid)
            .thenRun(collector)
            .exceptionally(collector);
   }

   private void checkCollector(XaResultCollector collector, boolean commit) {
      switch (collector.status()) {
         case ERROR:
         case HEURISTIC_MIXED:
            status = Status.STATUS_UNKNOWN;
            //some resources commits, other rollbacks and others we don't know...
            HeuristicMixedException exception = new HeuristicMixedException();
            collector.addSuppressedTo(exception);
            status = Status.STATUS_UNKNOWN;
            throw asCompletionException(exception);
         case HEURISTIC_ROLLBACK:
            HeuristicRollbackException e = new HeuristicRollbackException();
            collector.addSuppressedTo(e);
            status = Status.STATUS_UNKNOWN;
            throw asCompletionException(e);
         default:
            break;
      }
      status = commit ? Status.STATUS_COMMITTED : Status.STATUS_ROLLEDBACK;
   }

   private void checkStatusBeforeRegister(String component) throws RollbackException, IllegalStateException {
      if (status == Status.STATUS_MARKED_ROLLBACK) {
         throw new RollbackException("Transaction has been marked as rollback only");
      }
      if (isDone()) {
         throw new IllegalStateException(format("Transaction is done. Cannot register any more %s", component));
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

   private static class XaResourceData {
      final XAResource xaResource;
      volatile int status;

      private XaResourceData(XAResource xaResource) {
         this.xaResource = Objects.requireNonNull(xaResource);
      }
   }

   private enum TxCompletableStatus {
      NONE,
      OK,
      HEURISTIC_ROLLBACK,
      HEURISTIC_MIXED,
      ERROR


   }

   private static class XaResultCollector implements Runnable, Function<Throwable, Void> {
      private TxCompletableStatus status = TxCompletableStatus.NONE;
      private final List<Throwable> exceptions;
      private final boolean commit;

      XaResultCollector(int capacity, boolean commit) {
         exceptions = new ArrayList<>(capacity);
         this.commit = commit;
      }

      @Override
      public synchronized void run() {
         if (status == TxCompletableStatus.NONE) {
            status = TxCompletableStatus.OK;
         } else if (status == TxCompletableStatus.HEURISTIC_ROLLBACK) {
            status = TxCompletableStatus.HEURISTIC_MIXED;
         }
      }

      @Override
      public synchronized Void apply(Throwable throwable) {
         throwable = extractException(throwable);
         log.errorCommittingTx(throwable);
         exceptions.add(throwable);
         if (throwable instanceof XAException) {
            XAException e = (XAException) throwable;
            switch (e.errorCode) {
               case XAException.XA_HEURCOM:
               case XAException.XA_HEURRB:
               case XAException.XA_HEURMIX:
                  if (status == TxCompletableStatus.NONE) {
                     status = TxCompletableStatus.HEURISTIC_ROLLBACK;
                  } else if (status == TxCompletableStatus.OK) {
                     status = TxCompletableStatus.HEURISTIC_MIXED;
                  }
                  break;
               case XAException.XAER_NOTA:
                  if (commit) {
                     if (status == TxCompletableStatus.NONE) {
                        status = TxCompletableStatus.HEURISTIC_ROLLBACK;
                     } else if (status == TxCompletableStatus.OK) {
                        status = TxCompletableStatus.HEURISTIC_MIXED;
                     }
                  } else {
                     // we are rolling back the transaction but the transaction does no exist.
                     // just ignore it
                     if (status == TxCompletableStatus.NONE) {
                        status = TxCompletableStatus.OK;
                     }
                  }
                  break;
               default:
                  status = TxCompletableStatus.ERROR;
                  break;
            }
         } else {
            status = TxCompletableStatus.ERROR;
         }
         return null;
      }

      synchronized TxCompletableStatus status() {
         return status;
      }

      synchronized void addSuppressedTo(Throwable t) {
         exceptions.forEach(t::addSuppressed);
      }
   }
}
