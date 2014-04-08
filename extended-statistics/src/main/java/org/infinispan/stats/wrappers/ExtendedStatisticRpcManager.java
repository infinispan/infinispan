package org.infinispan.stats.wrappers;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.CacheStatisticManager;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.logging.Log;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.ExtendedStatistic.*;

/**
 * Takes statistics about the RPC invocations.
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
public class ExtendedStatisticRpcManager implements RpcManager {
   private static final Log log = LogFactory.getLog(ExtendedStatisticRpcManager.class, Log.class);
   private final RpcManager actual;
   private final CacheStatisticManager cacheStatisticManager;
   private final RpcDispatcher.Marshaller marshaller;
   private final TimeService timeService;

   public ExtendedStatisticRpcManager(RpcManager actual, CacheStatisticManager cacheStatisticManager,
                                      TimeService timeService) {
      this.actual = actual;
      this.cacheStatisticManager = cacheStatisticManager;
      Transport t = actual.getTransport();
      if (t instanceof JGroupsTransport) {
         marshaller = ((JGroupsTransport) t).getCommandAwareRpcDispatcher().getMarshaller();
      } else {
         marshaller = null;
      }
      this.timeService = timeService;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue,
                                                ResponseFilter responseFilter) {
      long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue,
                                                         responseFilter);
      updateStats(rpcCommand, mode.isSynchronous(), timeService.timeDuration(start, NANOSECONDS), recipients);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue) {
      long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
      updateStats(rpcCommand, mode.isSynchronous(), timeService.timeDuration(start, NANOSECONDS), recipients);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout) {
      long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout);
      updateStats(rpcCommand, mode.isSynchronous(), timeService.timeDuration(start, NANOSECONDS), recipients);
      return ret;
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws RpcException {
      long start = timeService.time();
      actual.broadcastRpcCommand(rpc, sync);
      updateStats(rpc, sync, timeService.timeDuration(start, NANOSECONDS), null);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue)
         throws RpcException {
      long start = timeService.time();
      actual.broadcastRpcCommand(rpc, sync, usePriorityQueue);
      updateStats(rpc, sync, timeService.timeDuration(start, NANOSECONDS), null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      long start = timeService.time();
      actual.broadcastRpcCommandInFuture(rpc, future);
      updateStats(rpc, false, timeService.timeDuration(start, NANOSECONDS), null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue,
                                           NotifyingNotifiableFuture<Object> future) {
      long start = timeService.time();
      actual.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
      updateStats(rpc, false, timeService.timeDuration(start, NANOSECONDS), null);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync)
         throws RpcException {
      long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpc, sync);
      updateStats(rpc, sync, timeService.timeDuration(start, NANOSECONDS), recipients);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync,
                                                boolean usePriorityQueue) throws RpcException {
      long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpc, sync, usePriorityQueue);
      updateStats(rpc, sync, timeService.timeDuration(start, NANOSECONDS), recipients);
      return ret;
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc,
                                      NotifyingNotifiableFuture<Object> future) {
      long start = timeService.time();
      actual.invokeRemotelyInFuture(recipients, rpc, future);
      updateStats(rpc, false, timeService.timeDuration(start, NANOSECONDS), recipients);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future) {
      long start = timeService.time();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
      updateStats(rpc, false, timeService.timeDuration(start, NANOSECONDS), recipients);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout) {
      long start = timeService.time();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
      updateStats(rpc, false, timeService.timeDuration(start, NANOSECONDS), recipients);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      long start = timeService.time();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout, ignoreLeavers);
      updateStats(rpc, false, timeService.timeDuration(start, NANOSECONDS), recipients);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      long start = timeService.time();
      Map<Address, Response> responseMap = actual.invokeRemotely(recipients, rpc, options);
      updateStats(rpc, options.responseMode().isSynchronous(), timeService.timeDuration(start, NANOSECONDS), recipients);
      return responseMap;
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options, NotifyingNotifiableFuture<Object> future) {
      long start = timeService.time();
      actual.invokeRemotelyInFuture(recipients, rpc, options, future);
      updateStats(rpc, options.responseMode().isSynchronous(), timeService.timeDuration(start, NANOSECONDS), recipients);
   }

   @Override
   public void invokeRemotelyInFuture(NotifyingNotifiableFuture<Map<Address, Response>> future, Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      long start = timeService.time();
      actual.invokeRemotelyInFuture(future, recipients, rpc, options);
      updateStats(rpc, options.responseMode().isSynchronous(), timeService.timeDuration(start, NANOSECONDS), recipients);
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode) {
      return actual.getRpcOptionsBuilder(responseMode);
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, boolean fifoOrder) {
      return actual.getRpcOptionsBuilder(responseMode, fifoOrder);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync) {
      return actual.getDefaultRpcOptions(sync);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync, boolean fifoOrder) {
      return actual.getDefaultRpcOptions(sync, fifoOrder);
   }

   @Override
   public Transport getTransport() {
      return actual.getTransport();
   }

   @Override
   public List<Address> getMembers() {
      return actual.getMembers();
   }

   @Override
   public Address getAddress() {
      return actual.getAddress();
   }

   @Override
   public int getTopologyId() {
      return actual.getTopologyId();
   }

   private void updateStats(ReplicableCommand command, boolean sync, long duration, Collection<Address> recipients) {
      ExtendedStatistic durationStat;
      ExtendedStatistic counterStat;
      ExtendedStatistic recipientSizeStat;
      ExtendedStatistic commandSizeStat = null;
      GlobalTransaction globalTransaction;

      if (command instanceof PrepareCommand) {
         durationStat = sync ? SYNC_PREPARE_TIME : ASYNC_PREPARE_TIME;
         counterStat = sync ? NUM_SYNC_PREPARE : NUM_ASYNC_PREPARE;
         recipientSizeStat = NUM_NODES_PREPARE;
         commandSizeStat = PREPARE_COMMAND_SIZE;
         globalTransaction = ((PrepareCommand) command).getGlobalTransaction();
      } else if (command instanceof RollbackCommand) {
         durationStat = sync ? SYNC_ROLLBACK_TIME : ASYNC_ROLLBACK_TIME;
         counterStat = sync ? NUM_SYNC_ROLLBACK : NUM_ASYNC_ROLLBACK;
         recipientSizeStat = NUM_NODES_ROLLBACK;
         globalTransaction = ((RollbackCommand) command).getGlobalTransaction();
      } else if (command instanceof CommitCommand) {
         durationStat = sync ? SYNC_COMMIT_TIME : ASYNC_COMMIT_TIME;
         counterStat = sync ? NUM_SYNC_COMMIT : NUM_ASYNC_COMMIT;
         recipientSizeStat = NUM_NODES_COMMIT;
         commandSizeStat = COMMIT_COMMAND_SIZE;
         globalTransaction = ((CommitCommand) command).getGlobalTransaction();
      } else if (command instanceof TxCompletionNotificationCommand) {
         durationStat = ASYNC_COMPLETE_NOTIFY_TIME;
         counterStat = NUM_ASYNC_COMPLETE_NOTIFY;
         recipientSizeStat = NUM_NODES_COMPLETE_NOTIFY;
         globalTransaction = ((TxCompletionNotificationCommand) command).getGlobalTransaction();
      } else if (command instanceof ClusteredGetCommand && !((ClusteredGetCommand) command).isWrite()) {
         durationStat = SYNC_GET_TIME;
         counterStat = NUM_SYNC_GET;
         recipientSizeStat = NUM_NODES_GET;
         commandSizeStat = CLUSTERED_GET_COMMAND_SIZE;
         globalTransaction = ((ClusteredGetCommand) command).getGlobalTransaction();
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Does not update stats for command %s. The command is not needed", command);
         }
         return;
      }

      if (log.isTraceEnabled()) {
         log.tracef("Update stats for command %s. Is sync? %s. Duration stat is %s, counter stats is %s, " +
                          "recipient size stat is %s", command, sync, durationStat, counterStat, recipientSizeStat);
      }
      cacheStatisticManager.add(durationStat, duration, globalTransaction, true);
      cacheStatisticManager.increment(counterStat, globalTransaction, true);
      cacheStatisticManager.add(recipientSizeStat, recipientListSize(recipients), globalTransaction, true);
      if (commandSizeStat != null) {
         cacheStatisticManager.add(commandSizeStat, getCommandSize(command), globalTransaction, true);
      }
   }

   private int recipientListSize(Collection<Address> recipients) {
      return recipients == null ? actual.getTransport().getMembers().size() : recipients.size();
   }

   private int getCommandSize(ReplicableCommand command) {
      try {
         Buffer buffer = marshaller.objectToBuffer(command);
         return buffer != null ? buffer.getLength() : 0;
      } catch (Exception e) {
         return 0;
      }
   }
}
