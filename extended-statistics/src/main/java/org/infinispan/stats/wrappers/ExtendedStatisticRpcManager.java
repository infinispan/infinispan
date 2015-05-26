package org.infinispan.stats.wrappers;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.ExtendedStatistic.*;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
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
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpc,
                                                                        RpcOptions options) {
      long start = timeService.time();
      CompletableFuture<Map<Address, Response>> future = actual.invokeRemotelyAsync(recipients, rpc, options);
      updateStats(rpc, options.responseMode().isSynchronous(), timeService.timeDuration(start, NANOSECONDS), recipients);
      return future;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      long start = timeService.time();
      Map<Address, Response> responseMap = actual.invokeRemotely(recipients, rpc, options);
      updateStats(rpc, options.responseMode().isSynchronous(), timeService.timeDuration(start, NANOSECONDS), recipients);
      return responseMap;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcs, RpcOptions options) {
      long start = timeService.time();
      Map<Address, Response> responseMap = actual.invokeRemotely(rpcs, options);
      for (Entry<Address, ReplicableCommand> entry : rpcs.entrySet()) {
         // TODO: This is giving a time for all rpcs combined...
         updateStats(entry.getValue(), options.responseMode().isSynchronous(),
               timeService.timeDuration(start, NANOSECONDS), Collections.singleton(entry.getKey()));
      }
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
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, DeliverOrder deliverOrder) {
      return actual.getRpcOptionsBuilder(responseMode, deliverOrder);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync) {
      return actual.getDefaultRpcOptions(sync);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync, DeliverOrder deliverOrder) {
      return actual.getDefaultRpcOptions(sync, deliverOrder);
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
