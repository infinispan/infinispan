package org.infinispan.extendedstats.wrappers;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.infinispan.commons.time.TimeService;
import org.infinispan.extendedstats.CacheStatisticManager;
import org.infinispan.extendedstats.container.ExtendedStatistic;
import org.infinispan.extendedstats.logging.Log;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

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
   private final boolean trace = log.isTraceEnabled();
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
         updateStats(rpc, true, timeService.timeDuration(start, NANOSECONDS), recipients);
         return responseMap;
      });
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder) {
      actual.sendTo(destination, command, deliverOrder);
   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand command, DeliverOrder deliverOrder) {
      if (command instanceof TxCompletionNotificationCommand) {
         long start = timeService.time();
         actual.sendToMany(destinations, command, deliverOrder);
         updateStats(command, false, timeService.timeDuration(start, NANOSECONDS), destinations);
      } else {
         actual.sendToMany(destinations, command, deliverOrder);
      }
   }

   @Override
   public void sendToAll(ReplicableCommand command, DeliverOrder deliverOrder) {
      actual.sendToAll(command, deliverOrder);
   }

   @Override
   public <O> XSiteResponse<O> invokeXSite(XSiteBackup backup, XSiteReplicateCommand<O> command) {
      return actual.invokeXSite(backup, command);
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
         durationStat = ExtendedStatistic.SYNC_PREPARE_TIME;
         counterStat = ExtendedStatistic.NUM_SYNC_PREPARE;
         recipientSizeStat = ExtendedStatistic.NUM_NODES_PREPARE;
         commandSizeStat = ExtendedStatistic.PREPARE_COMMAND_SIZE;
         globalTransaction = ((PrepareCommand) command).getGlobalTransaction();
      } else if (command instanceof RollbackCommand) {
         durationStat = ExtendedStatistic.SYNC_ROLLBACK_TIME;
         counterStat =  ExtendedStatistic.NUM_SYNC_ROLLBACK;
         recipientSizeStat = ExtendedStatistic.NUM_NODES_ROLLBACK;
         globalTransaction = ((RollbackCommand) command).getGlobalTransaction();
      } else if (command instanceof CommitCommand) {
         durationStat = ExtendedStatistic.SYNC_COMMIT_TIME;
         counterStat = ExtendedStatistic.NUM_SYNC_COMMIT;
         recipientSizeStat = ExtendedStatistic.NUM_NODES_COMMIT;
         commandSizeStat = ExtendedStatistic.COMMIT_COMMAND_SIZE;
         globalTransaction = ((CommitCommand) command).getGlobalTransaction();
      } else if (command instanceof TxCompletionNotificationCommand) {
         durationStat = ExtendedStatistic.ASYNC_COMPLETE_NOTIFY_TIME;
         counterStat = ExtendedStatistic.NUM_ASYNC_COMPLETE_NOTIFY;
         recipientSizeStat = ExtendedStatistic.NUM_NODES_COMPLETE_NOTIFY;
         globalTransaction = ((TxCompletionNotificationCommand) command).getGlobalTransaction();
      } else if (command instanceof ClusteredGetCommand && !((ClusteredGetCommand) command).isWrite()) {
         durationStat = ExtendedStatistic.SYNC_GET_TIME;
         counterStat = ExtendedStatistic.NUM_SYNC_GET;
         recipientSizeStat = ExtendedStatistic.NUM_NODES_GET;
         commandSizeStat = ExtendedStatistic.CLUSTERED_GET_COMMAND_SIZE;
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

   private static final class CountingDataOutput extends OutputStream {

      private int count = 0;

      public int getCount() {
         return count;
      }

      @Override
      public void write(int b) {
         count++;
      }

      @Override
      public void write(byte[] b) {
         count += b.length;
      }

      @Override
      public void write(byte[] b, int off, int len) {
         count += len;
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
   }
}
