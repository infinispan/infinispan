package org.infinispan.stats.wrappers;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.ExtendedStatistic.ASYNC_COMPLETE_NOTIFY_TIME;
import static org.infinispan.stats.container.ExtendedStatistic.CLUSTERED_GET_COMMAND_SIZE;
import static org.infinispan.stats.container.ExtendedStatistic.COMMIT_COMMAND_SIZE;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_ASYNC_COMPLETE_NOTIFY;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_NODES_COMMIT;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_NODES_COMPLETE_NOTIFY;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_NODES_GET;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_NODES_PREPARE;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_NODES_ROLLBACK;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_SYNC_COMMIT;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_SYNC_GET;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_SYNC_PREPARE;
import static org.infinispan.stats.container.ExtendedStatistic.NUM_SYNC_ROLLBACK;
import static org.infinispan.stats.container.ExtendedStatistic.PREPARE_COMMAND_SIZE;
import static org.infinispan.stats.container.ExtendedStatistic.SYNC_COMMIT_TIME;
import static org.infinispan.stats.container.ExtendedStatistic.SYNC_GET_TIME;
import static org.infinispan.stats.container.ExtendedStatistic.SYNC_PREPARE_TIME;
import static org.infinispan.stats.container.ExtendedStatistic.SYNC_ROLLBACK_TIME;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.stats.CacheStatisticManager;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.logging.Log;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.LogFactory;

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
   private static final boolean trace = log.isTraceEnabled();
   private final RpcManager actual;
   private final CacheStatisticManager cacheStatisticManager;
   private final org.infinispan.commons.marshall.StreamingMarshaller marshaller;
   private final TimeService timeService;

   public ExtendedStatisticRpcManager(RpcManager actual, CacheStatisticManager cacheStatisticManager,
                                      TimeService timeService, StreamingMarshaller marshaller) {
      this.actual = actual;
      this.cacheStatisticManager = cacheStatisticManager;
      this.marshaller = marshaller;
      this.timeService = timeService;
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                               ResponseCollector<T> collector, RpcOptions rpcOptions) {
      long start = timeService.time();
      CompletionStage<T> request = actual.invokeCommand(target, command, collector, rpcOptions);
      return request.thenApply(responseMap -> {
         updateStats(command, true, timeService.timeDuration(start, NANOSECONDS), Collections.singleton(target));
         return responseMap;
      });
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                               ResponseCollector<T> collector, RpcOptions rpcOptions) {
      long start = timeService.time();
      CompletionStage<T> request = actual.invokeCommand(targets, command, collector, rpcOptions);
      return request.thenApply(responseMap -> {
         updateStats(command, true, timeService.timeDuration(start, NANOSECONDS), targets);
         return responseMap;
      });
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                    RpcOptions rpcOptions) {
      long start = timeService.time();
      CompletionStage<T> request = actual.invokeCommandOnAll(command, collector, rpcOptions);
      return request.thenApply(responseMap -> {
         updateStats(command, true, timeService.timeDuration(start, NANOSECONDS), actual.getTransport().getMembers());
         return responseMap;
      });
   }

   @Override
   public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                        ResponseCollector<T> collector, RpcOptions rpcOptions) {
      long start = timeService.time();
      CompletionStage<T> request = actual.invokeCommandStaggered(targets, command, collector, rpcOptions);
      return request.thenApply(responseMap -> {
         updateStats(command, true, timeService.timeDuration(start, NANOSECONDS), targets);
         return responseMap;
      });
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                Function<Address, ReplicableCommand> commandGenerator,
                                                ResponseCollector<T> collector, RpcOptions rpcOptions) {
      long start = timeService.time();
      CompletionStage<T> request = actual.invokeCommands(targets, commandGenerator, collector, rpcOptions);
      return request.thenApply(responseMap -> {
         targets.forEach(
               target -> updateStats(commandGenerator.apply(target), true, timeService.timeDuration(start, NANOSECONDS),
                                     Collections.singleton(target)));
         return responseMap;
      });
   }

   @Override
   public <T> T blocking(CompletionStage<T> request) {
      return actual.blocking(request);
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpc,
                                                                        RpcOptions options) {
      long start = timeService.time();
      CompletableFuture<Map<Address, Response>> future = actual.invokeRemotelyAsync(recipients, rpc, options);
      return future.thenApply(responseMap -> {
         updateStats(rpc, options.responseMode().isSynchronous(), timeService.timeDuration(start, NANOSECONDS),
                     recipients);
         return responseMap;
      });
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
   public void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder) {
      actual.sendTo(destination, command, deliverOrder);
   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand command, DeliverOrder deliverOrder) {
      actual.sendToMany(destinations, command, deliverOrder);
   }

   @Override
   public void sendToAll(ReplicableCommand command, DeliverOrder deliverOrder) {
      actual.sendToAll(command, deliverOrder);
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

   @Override
   public RpcOptions getSyncRpcOptions() {
      return actual.getSyncRpcOptions();
   }

   @Override
   public RpcOptions getTotalSyncRpcOptions() {
      return actual.getTotalSyncRpcOptions();
   }

   private void updateStats(ReplicableCommand command, boolean sync, long duration, Collection<Address> recipients) {
      ExtendedStatistic durationStat;
      ExtendedStatistic counterStat;
      ExtendedStatistic recipientSizeStat;
      ExtendedStatistic commandSizeStat = null;
      GlobalTransaction globalTransaction;

      if (command instanceof PrepareCommand) {
         durationStat = SYNC_PREPARE_TIME;
         counterStat = NUM_SYNC_PREPARE;
         recipientSizeStat = NUM_NODES_PREPARE;
         commandSizeStat = PREPARE_COMMAND_SIZE;
         globalTransaction = ((PrepareCommand) command).getGlobalTransaction();
      } else if (command instanceof RollbackCommand) {
         durationStat = SYNC_ROLLBACK_TIME;
         counterStat =  NUM_SYNC_ROLLBACK;
         recipientSizeStat = NUM_NODES_ROLLBACK;
         globalTransaction = ((RollbackCommand) command).getGlobalTransaction();
      } else if (command instanceof CommitCommand) {
         durationStat = SYNC_COMMIT_TIME;
         counterStat = NUM_SYNC_COMMIT;
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
         globalTransaction = null;
      } else {
         if (trace) {
            log.tracef("Does not update stats for command %s. The command is not needed", command);
         }
         return;
      }

      if (trace) {
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
         CountingDataOutput dataOutput = new CountingDataOutput();
         ObjectOutput byteOutput = marshaller.startObjectOutput(dataOutput, false, 0);
         marshaller.objectToObjectStream(command, byteOutput);
         marshaller.finishObjectOutput(byteOutput);
         return dataOutput.getCount();
      } catch (Exception e) {
         return 0;
      }
   }

   private static class CountingDataOutput extends OutputStream {
      private int count;

      private CountingDataOutput() {
         this.count = 0;
      }

      public int getCount() {
         return count;
      }

      @Override
      public void write(int b) throws IOException {
         count++;
      }

      @Override
      public void write(byte[] b) throws IOException {
         count += b.length;
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
         count += len;
      }

      @Override
      public void flush() throws IOException {

      }

      @Override
      public void close() throws IOException {

      }
   }
}
