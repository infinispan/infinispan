package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationFinallyFunction;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A base class for a state transfer interceptor. It contains the base code to avoid duplicating in the two current
 * different implementations.
 * <p/>
 * Also, it has some utilities methods with the most common logic.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public abstract class BaseStateTransferInterceptor extends DDAsyncInterceptor {
   private final InvocationFinallyFunction<VisitableCommand> handleReadCommandReturn = this::handleReadCommandReturn;

   @Inject Configuration configuration;
   @Inject protected StateTransferLock stateTransferLock;
   @Inject @ComponentName(KnownComponentNames.NON_BLOCKING_EXECUTOR)
   Executor nonBlockingExecutor;
   @Inject DistributionManager distributionManager;
   @Inject @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService timeoutExecutor;

   private long transactionDataTimeout;

   @Start
   public void start() {
      transactionDataTimeout = configuration.clustering().remoteTimeout();
      configuration.clustering().attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
                   .addListener((a, ignored) -> {
                      transactionDataTimeout = a.get();
                   });
   }

   protected final void logRetry(int currentTopologyId, TopologyAffectedCommand cmd) {
      if (getLog().isTraceEnabled())
         getLog().tracef("Retrying command because of topology change, current topology is %d, command topology %d: %s",
                         currentTopologyId, cmd.getTopologyId(), cmd);
   }

   protected final int currentTopologyId() {
      final CacheTopology cacheTopology = distributionManager.getCacheTopology();
      return cacheTopology == null ? -1 : cacheTopology.getTopologyId();
   }

   protected final void updateTopologyId(TopologyAffectedCommand command) {
      // set the topology id if it was not set before (ie. this is local command)
      // TODO Make tx commands extend FlagAffectedCommand so we can use CACHE_MODE_LOCAL in TransactionTable.cleanupStaleTransactions
      if (command.getTopologyId() == -1) {
         CacheTopology cacheTopology = distributionManager.getCacheTopology();
         // Before the topology is set in STM/StateConsumer the topology in DistributionManager is 0
         int topologyId = cacheTopology == null ? 0 : cacheTopology.getTopologyId();
         if (getLog().isTraceEnabled()) getLog().tracef("Setting command topology to %d", topologyId);
         command.setTopologyId(topologyId);
      }
   }

   protected <T extends VisitableCommand> Object retryWhenDone(CompletionStage<Void> stage, int topologyId,
                                                               InvocationContext ctx, T command,
                                                               InvocationFinallyFunction<T> callback) {
      CompletableFuture<Void> future = stage.toCompletableFuture();
      if (future.toCompletableFuture().isDone()) {
         getLog().tracef("Retrying command %s for topology %d", command, topologyId);
         return invokeNextAndHandle(ctx, command, callback);
      } else {
         CancellableRetry<T> cancellableRetry = new CancellableRetry<>(command, topologyId);
         // We have to use handleAsync and rethrow the exception in the handler, rather than
         // thenComposeAsync(), because if `future` completes with an exception we want to continue in remoteExecutor
         CompletableFuture<Void> retryFuture = future.handleAsync(cancellableRetry, nonBlockingExecutor);
         cancellableRetry.setRetryFuture(retryFuture);
         // We want to time out the current command future, not the main topology-waiting future,
         // but the command future can take longer time to finish.
         ScheduledFuture<?> timeoutFuture = timeoutExecutor.schedule(cancellableRetry, transactionDataTimeout, TimeUnit.MILLISECONDS);
         cancellableRetry.setTimeoutFuture(timeoutFuture);
         return makeStage(asyncInvokeNext(ctx, command, retryFuture)).andHandle(ctx, command, callback);
      }
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   protected <C extends VisitableCommand & TopologyAffectedCommand & FlagAffectedCommand> Object handleReadCommand(
         InvocationContext ctx, C command) {
      updateTopologyId(command);
      return invokeNextAndHandle(ctx, command, handleReadCommandReturn);
   }

   private Object handleExceptionOnReadCommandReturn(InvocationContext rCtx, VisitableCommand rCommand, Throwable t) throws Throwable {
      Throwable ce = t;
      while (ce instanceof RemoteException) {
         ce = ce.getCause();
      }
      TopologyAffectedCommand cmd = (TopologyAffectedCommand) rCommand;
      final CacheTopology cacheTopology = distributionManager.getCacheTopology();
      int currentTopologyId = cacheTopology.getTopologyId();
      int requestedTopologyId;
      if (ce instanceof SuspectException) {
         // Read commands must ignore CacheNotFoundResponses
         throw new IllegalStateException("Read commands must ignore leavers");
      } else if (ce instanceof OutdatedTopologyException) {
         logRetry(currentTopologyId, cmd);

         // We can get OTE for dist reads even if current topology information is sufficient:
         // 1. A has topology in phase READ_ALL_WRITE_ALL, sends message to both old owner B and new C
         // 2. C has old topology with READ_OLD_WRITE_ALL, so it responds with UnsureResponse
         // 3. C updates topology to READ_ALL_WRITE_ALL, B updates to READ_NEW_WRITE_ALL
         // 4. B receives the read, but it already can't read: responds with UnsureResponse
         // 5. A receives two unsure responses and throws OTE
         // However, now we are sure that we can immediately retry the request, because C must have updated its topology
         OutdatedTopologyException ote = (OutdatedTopologyException) ce;
         requestedTopologyId = cmd.getTopologyId() + ote.topologyIdDelta;
      } else if (ce instanceof AllOwnersLostException) {
         if (getLog().isTraceEnabled())
            getLog().tracef("All owners for command %s have been lost.", cmd);
         // During partition the exception is already handled in PartitionHandlingInterceptor,
         // and if the handling is not enabled, we can't but return null.
         requestedTopologyId = cmd.getTopologyId() + 1;
      } else {
         throw t;
      }
      // Only retry once if currentTopologyId > cmdTopologyId + 1
      int retryTopologyId = Math.max(currentTopologyId, requestedTopologyId);
      cmd.setTopologyId(retryTopologyId);
      ((FlagAffectedCommand) cmd).addFlags(FlagBitSets.COMMAND_RETRY);
      if (retryTopologyId == currentTopologyId) {
         return invokeNextAndHandle(rCtx, rCommand, handleReadCommandReturn);
      } else {
         return makeStage(asyncInvokeNext(rCtx, rCommand, stateTransferLock.transactionDataFuture(retryTopologyId)))
               .andHandle(rCtx, rCommand, handleReadCommandReturn);
      }
   }

   private Object handleReadCommandReturn(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable t)
         throws Throwable {
      if (t == null)
         return rv;

      // Separate method to allow for inlining of this method since exception should rarely occur
      return handleExceptionOnReadCommandReturn(rCtx, rCommand, t);
   }

   protected int getNewTopologyId(Throwable ce, int currentTopologyId, TopologyAffectedCommand command) {
      int requestedDelta;
      if (ce instanceof OutdatedTopologyException) {
         requestedDelta = ((OutdatedTopologyException) ce).topologyIdDelta;
      } else {
         // SuspectException
         requestedDelta = 1;
      }
      return Math.max(currentTopologyId, command.getTopologyId() + requestedDelta);
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   protected abstract Log getLog();

   private static class CancellableRetry<T extends VisitableCommand> implements BiFunction<Void, Throwable, Void>, Runnable {
      private static final AtomicReferenceFieldUpdater<CancellableRetry, Throwable> cancellableRetryUpdater
            = AtomicReferenceFieldUpdater.newUpdater(CancellableRetry.class, Throwable.class, "cancelled");
      private static final AtomicReferenceFieldUpdater<CancellableRetry, Object> timeoutFutureUpdater
            = AtomicReferenceFieldUpdater.newUpdater(CancellableRetry.class, Object.class, "timeoutFuture");

      private static final Log log = LogFactory.getLog(CancellableRetry.class);
      private static final Throwable DUMMY = new Throwable("Command is retried"); // should not be ever thrown

      private final T command;
      private final int topologyId;
      private volatile Throwable cancelled = null;
      // retryFuture is not volatile because it is used only in the timeout handler = run()
      // and that is scheduled after retryFuture is set
      private CompletableFuture<Void> retryFuture;
      // ScheduledFuture does not have any dummy implementations, so we'll use plain Object as the field
      @SuppressWarnings("unused")
      private volatile Object timeoutFuture;

      CancellableRetry(T command, int topologyId) {
         this.command = command;
         this.topologyId = topologyId;
      }

      /**
       * This is called when the topology future completes (successfully or exceptionally)
       */
      @Override
      public Void apply(Void nil, Throwable throwable) {
         if (!timeoutFutureUpdater.compareAndSet(this, null, DUMMY)) {
            ((ScheduledFuture) timeoutFuture).cancel(false);
         }

         if (throwable != null) {
            throw CompletableFutures.asCompletionException(throwable);
         }
         if (!cancellableRetryUpdater.compareAndSet(this, null, DUMMY)) {
            log.tracef("Not retrying command %s as it has been cancelled.", command);
            throw CompletableFutures.asCompletionException(cancelled);
         }
         log.tracef("Retrying command %s for topology %d", command, topologyId);
         return null;
      }

      /**
       * This is called when the timeout elapses.
       */
      @Override
      public void run() {
         TimeoutException timeoutException = new TimeoutException("Timed out waiting for topology " + topologyId);
         if (cancellableRetryUpdater.compareAndSet(this, null, timeoutException)) {
            retryFuture.completeExceptionally(timeoutException);
         }
      }

      void setRetryFuture(CompletableFuture<Void> retryFuture) {
         this.retryFuture = retryFuture;
      }

      void setTimeoutFuture(ScheduledFuture<?> timeoutFuture) {
         if (!timeoutFutureUpdater.compareAndSet(this, null, timeoutFuture)) {
            timeoutFuture.cancel(false);
         }
      }
   }

   // We don't need to implement GetAllCommand or ReadManyCommand here because these don't throw AllOwnersLostException
   protected static class LostDataVisitor extends AbstractVisitor {
      public static final LostDataVisitor INSTANCE = new LostDataVisitor();

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         return null;
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         return null;
      }

      @Override
      public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
         return command.performOnLostData();
      }
   }
}
