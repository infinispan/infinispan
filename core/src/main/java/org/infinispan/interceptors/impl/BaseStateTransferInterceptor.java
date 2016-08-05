package org.infinispan.interceptors.impl;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
   private final boolean trace = getLog().isTraceEnabled();

   protected StateTransferManager stateTransferManager;
   protected StateTransferLock stateTransferLock;
   private GroupManager groupManager;
   private long transactionDataTimeout;
   private ScheduledExecutorService timeoutExecutor;

   @Inject
   public void init(StateTransferLock stateTransferLock, Configuration configuration,
                    StateTransferManager stateTransferManager, GroupManager groupManager,
                    @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor) {
      this.stateTransferLock = stateTransferLock;
      this.stateTransferManager = stateTransferManager;
      this.groupManager = groupManager;
      this.timeoutExecutor = timeoutExecutor;
      transactionDataTimeout = configuration.clustering().remoteTimeout();
   }

   @Override
   public CompletableFuture<Void> visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      final String groupName = command.getGroupName();
      final boolean isOwner = groupManager.isOwner(groupName);
      updateTopologyId(command);
      final int commandTopologyId = command.getTopologyId();

      if (ctx.isOriginLocal()) {
         return ctx.forkInvocation(command, (rCtx, rCommand, rv, throwable) -> {
            boolean shouldRetry;
            if (throwable != null) {
               // Must retry if we got an OutdatedTopologyException or SuspectException
               Throwable ce = throwable;
               while (ce instanceof RemoteException) {
                  ce = ce.getCause();
               }
               shouldRetry = ce instanceof OutdatedTopologyException || ce instanceof SuspectException;
            } else {
               // Only check the topology id if if we are an owner
               shouldRetry = isOwner && currentTopologyId() != commandTopologyId;
            }
            if (shouldRetry) {
               return retryVisitGetKeysInGroupCommand(ctx, command, commandTopologyId);
            }
            // No retry, either rethrow the exception or return the current result
            if (throwable != null) {
               throw throwable;
            } else {
               return ctx.shortCircuit(rv);
            }
         });
      } else {
         return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
            if (isOwner && currentTopologyId() != commandTopologyId) {
               throw new OutdatedTopologyException(
                     "Cache topology changed while the command was executing: expected " +
                           commandTopologyId + ", got " + currentTopologyId());
            }
            return null;
         });
      }
   }

   protected final void logRetry(VisitableCommand command) {
      if (trace) {
         getLog().tracef("Retrying command because of topology change: %s", command);
      }
   }

   protected final int currentTopologyId() {
      final CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      return cacheTopology == null ? -1 : cacheTopology.getTopologyId();
   }

   protected final void updateTopologyId(TopologyAffectedCommand command) throws InterruptedException {
      // set the topology id if it was not set before (ie. this is local command)
      // TODO Make tx commands extend FlagAffectedCommand so we can use CACHE_MODE_LOCAL in TransactionTable.cleanupStaleTransactions
      if (command.getTopologyId() == -1) {
         CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
         if (cacheTopology != null) {
            command.setTopologyId(cacheTopology.getTopologyId());
         }
      }
   }

   private CompletableFuture<Void> retryVisitGetKeysInGroupCommand(InvocationContext context, GetKeysInGroupCommand command,
                                                  int commandTopologyId) throws Throwable {
      logRetry(command);
      // We increment the topology id so that updateTopologyIdAndWaitForTransactionData waits for the next topology.
      // Without this, we could retry the command too fast and we could get the OutdatedTopologyException again.
      int newTopologyId = Math.max(currentTopologyId(), commandTopologyId + 1);
      command.setTopologyId(newTopologyId);
      return retryCommandWithTransactionData(newTopologyId, context, command, (ctx, cmd) -> visitGetKeysInGroupCommand(ctx, cmd));

   }

   protected <T extends VisitableCommand> CompletableFuture<Void> retryCommandWithTransactionData(int topologyId,
        InvocationContext ctx, T command, RetryHandler<T> handler) throws Throwable {
      CompletableFuture<Void> future = stateTransferLock.transactionDataFuture(topologyId);
      if (future == null) {
         getLog().tracef("Retrying command %s for topology %d", command, topologyId);
         return handler.handle(ctx, command);
      } else {
         CompletableFuture<Void> retryFuture = future.thenCompose(ignored -> {
            try {
               getLog().tracef("Retrying command %s for topology %d", command, topologyId);
               return handler.handle(ctx, command);
            } catch (Throwable t) {
               throw new CacheException(t);
            }
         });
         // We want to time out the current command future, not the main topology-waiting future,
         // but the command future can take longer time to finish.
         // This scheduled future does not have to be cancelled because it won't do anything (it will
         // sit idle in the scheduler queue).
         timeoutExecutor.schedule(() -> {
               if (!future.isDone()) {
                  retryFuture.completeExceptionally(new TimeoutException("Timed out waiting for topology " + topologyId));
               }
            }, transactionDataTimeout, TimeUnit.MILLISECONDS);
         return retryFuture;
      }
   }

   protected interface RetryHandler<T extends VisitableCommand> {
      CompletableFuture<Void> handle(InvocationContext ctx, T command) throws Throwable;
   }

   protected abstract Log getLog();
}
