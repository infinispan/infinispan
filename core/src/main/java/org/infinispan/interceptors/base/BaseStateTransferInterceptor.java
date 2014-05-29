package org.infinispan.interceptors.base;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;

import java.util.concurrent.TimeUnit;

/**
 * A base class for a state transfer interceptor. It contains the base code to avoid duplicating in the two current
 * different implementations.
 * <p/>
 * Also, it has some utilities methods with the most common logic.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BaseStateTransferInterceptor extends CommandInterceptor {

   protected StateTransferManager stateTransferManager;
   private StateTransferLock stateTransferLock;
   private GroupManager groupManager;
   private long transactionDataTimeout;

   @Inject
   public void init(StateTransferLock stateTransferLock, Configuration configuration,
                    StateTransferManager stateTransferManager, GroupManager groupManager) {
      this.stateTransferLock = stateTransferLock;
      this.stateTransferManager = stateTransferManager;
      this.groupManager = groupManager;
      transactionDataTimeout = configuration.clustering().sync().replTimeout();
   }

   @Override
   public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      final String groupName = command.getGroupName();
      final boolean isOwner = groupManager.isOwner(groupName);
      updateTopologyId(command);
      final int commandTopologyId = command.getTopologyId();

      if (ctx.isOriginLocal()) {
         //invoke next and check for exception
         Object localResult;
         try {
            localResult = invokeNextInterceptor(ctx, command);
            //if we are not the owner, we rely on the reply from the primary owner.
            if (isOwner && currentTopologyId() != commandTopologyId) {
               localResult = retryVisitGetKeysInGroupCommand(ctx, command, commandTopologyId);
            }
         } catch (CacheException e) {
            Throwable ce = e;
            while (ce instanceof RemoteException) {
               ce = ce.getCause();
            }
            if (!(ce instanceof OutdatedTopologyException) && !(ce instanceof SuspectException))
               throw e;

            localResult = retryVisitGetKeysInGroupCommand(ctx, command, commandTopologyId);
         }
         return localResult;
      } else {
         Object result = invokeNextInterceptor(ctx, command);
         if (isOwner && currentTopologyId() != commandTopologyId) {
            throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
                                                      commandTopologyId + ", got " + currentTopologyId());
         }
         return result;
      }
   }

   protected final void logRetry(VisitableCommand command) {
      final Log log = getLog();
      if (log.isTraceEnabled()) {
         log.tracef("Retrying command because of topology change: %s", command);
      }
   }

   protected final int currentTopologyId() {
      final CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      return cacheTopology == null ? -1 : cacheTopology.getTopologyId();
   }

   protected final void waitForTransactionData(int topologyId) throws InterruptedException {
      stateTransferLock.waitForTransactionData(topologyId, transactionDataTimeout, TimeUnit.MILLISECONDS);
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

   private Object retryVisitGetKeysInGroupCommand(InvocationContext context, GetKeysInGroupCommand command,
                                                  int commandTopologyId) throws Throwable {
      logRetry(command);
      // We increment the topology id so that updateTopologyIdAndWaitForTransactionData waits for the next topology.
      // Without this, we could retry the command too fast and we could get the OutdatedTopologyException again.
      int newTopologyId = Math.max(currentTopologyId(), commandTopologyId + 1);
      command.setTopologyId(newTopologyId);
      waitForTransactionData(newTopologyId);
      return visitGetKeysInGroupCommand(context, command);
   }
}
