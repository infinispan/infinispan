package org.infinispan.server.hotrod.tx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.topology.CacheTopology;

/**
 * It invokes a {@link TransactionBoundaryCommand} in all the cluster members, the local node included.
 * <p>
 * It is used to complete a client transaction whose originator left the cluster. Since the originator is gone, the Hot
 * Rod server replays the transaction on its behalf and, as the originator would do, it has to cope with concurrent
 * topology changes: a member that already installed a newer {@link CacheTopology} replies with {@link UnsureResponse}
 * and does not apply the command. When that happens, the command must be retried with the newer topology, otherwise the
 * transaction's modifications are silently lost in those members.
 *
 * @since 16.3
 */
public final class TxCommandInvoker {

   private static final Log log = Log.getLog(TxCommandInvoker.class);

   private TxCommandInvoker() {
   }

   /**
    * It invokes {@code command} in all the cluster members and retries it, with a newer topology, while a member
    * replies it is not able to apply it.
    *
    * @param registry   The cache's {@link ComponentRegistry}.
    * @param command    The {@link TransactionBoundaryCommand} to invoke.
    * @param topologyId The {@link CacheTopology} id in which the command must be applied.
    * @return A {@link CompletionStage} that completes when the command is applied in all the cluster members.
    */
   public static CompletionStage<Void> invokeInCluster(ComponentRegistry registry, TransactionBoundaryCommand command,
         int topologyId) {
      command.setTopologyId(topologyId);
      RpcManager rpcManager = registry.getComponent(RpcManager.class);
      CompletionStage<Boolean> remoteStage = rpcManager == null ?
            CompletableFutures.completedFalse() :
            rpcManager.invokeCommandOnAll(command, new OutdatedTopologyCollector(), rpcManager.getSyncRpcOptions());
      CompletionStage<Boolean> localStage = invokeLocally(registry, command);
      return remoteStage.thenCombine(localStage, Boolean::logicalOr)
            .thenCompose(outdatedTopology -> outdatedTopology ?
                  retry(registry, command, topologyId) :
                  CompletableFutures.completedNull());
   }

   private static CompletionStage<Boolean> invokeLocally(ComponentRegistry registry,
         TransactionBoundaryCommand command) {
      try {
         CompletionStage<?> stage = command.invokeAsync(registry);
         return stage.thenApply(rv -> rv == UnsureResponse.INSTANCE);
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(t);
      }
   }

   private static CompletionStage<Void> retry(ComponentRegistry registry, TransactionBoundaryCommand command,
         int topologyId) {
      //the command is only applied by the members in the same topology, so we need to move forward.
      int newTopologyId = Math.max(currentTopologyId(registry), topologyId + 1);
      if (log.isTraceEnabled()) {
         log.tracef("Retrying command %s in topology %s", command, newTopologyId);
      }
      if (command instanceof PrepareCommand prepareCommand) {
         prepareCommand.setRetriedCommand(true);
      }
      //it fails with a TimeoutException if the topology isn't installed in time. it avoids retrying forever.
      return registry.getStateTransferLock().transactionDataFuture(newTopologyId)
            .thenCompose(ignored -> invokeInCluster(registry, command, newTopologyId));
   }

   private static int currentTopologyId(ComponentRegistry registry) {
      var distributionManager = registry.getDistributionManager();
      CacheTopology topology = distributionManager == null ? null : distributionManager.getCacheTopology();
      return topology == null ? -1 : topology.getTopologyId();
   }

   /**
    * It collects the responses and returns {@code true} if, at least, one member was not able to apply the command.
    */
   private static final class OutdatedTopologyCollector extends ValidResponseCollector<Boolean> {

      private Exception exception;
      private boolean outdatedTopology;

      @Override
      protected Boolean addValidResponse(Address sender, ValidResponse<?> response) {
         if (response == UnsureResponse.INSTANCE) {
            if (log.isTraceEnabled()) {
               log.tracef("Node %s has a newer topology installed.", sender);
            }
            outdatedTopology = true;
         }
         return null;
      }

      @Override
      protected Boolean addTargetNotFound(Address sender) {
         recordException(ResponseCollectors.remoteNodeSuspected(sender));
         return null;
      }

      @Override
      protected Boolean addException(Address sender, Exception exception) {
         recordException(ResponseCollectors.wrapRemoteException(sender, exception));
         return null;
      }

      @Override
      public Boolean finish() {
         if (exception != null) {
            throw CompletableFutures.asCompletionException(exception);
         }
         return outdatedTopology;
      }

      private void recordException(Exception e) {
         if (exception == null) {
            exception = e;
         } else {
            exception.addSuppressed(e);
         }
      }
   }
}
