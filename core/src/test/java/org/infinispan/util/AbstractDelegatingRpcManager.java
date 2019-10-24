package org.infinispan.util;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.SingletonMapResponseCollector;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * Common rpc manager controls
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class AbstractDelegatingRpcManager implements RpcManager {
   protected final RpcManager realOne;

   public AbstractDelegatingRpcManager(RpcManager realOne) {
      this.realOne = realOne;
   }

   @Override
   public final <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                                     ResponseCollector<T> collector, RpcOptions rpcOptions) {
      return performRequest(Collections.singleton(target), command, collector,
                            c -> realOne.invokeCommand(target, command, c, rpcOptions), rpcOptions);
   }

   @Override
   public final <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                                     ResponseCollector<T> collector, RpcOptions rpcOptions) {
      return performRequest(targets, command, collector,
                            c -> realOne.invokeCommand(targets, command, c, rpcOptions), rpcOptions);
   }

   @Override
   public final <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                          RpcOptions rpcOptions) {
      return performRequest(getTransport().getMembers(), command, collector,
                            c -> realOne.invokeCommandOnAll(command, c, rpcOptions), rpcOptions);
   }

   @Override
   public final <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                              ResponseCollector<T> collector, RpcOptions rpcOptions) {
      return performRequest(targets, command, collector,
                            c -> realOne.invokeCommand(targets, command, c, rpcOptions), rpcOptions);
   }

   @Override
   public final <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                      Function<Address, ReplicableCommand> commandGenerator,
                                                      ResponseCollector<T> collector, RpcOptions rpcOptions) {
      // Split the invocation into multiple unicast requests
      CommandsRequest<T> action = new CommandsRequest<>(targets, collector);
      for (Address target : targets) {
         if (rpcOptions.deliverOrder() != DeliverOrder.TOTAL && target.equals(realOne.getAddress()))
            continue;

         invokeCommand(target, commandGenerator.apply(target), SingletonMapResponseCollector.ignoreLeavers(),
                       rpcOptions)
            .whenComplete(action);
      }
      return action.resultFuture;
   }

   @Override
   public final <T> T blocking(CompletionStage<T> request) {
      return realOne.blocking(request);
   }

   @Deprecated
   @Override
   public final CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                              ReplicableCommand command,
                                                                              RpcOptions rpcOptions) {
      Collection<Address> targets = recipients != null ? recipients : getTransport().getMembers();
      setTopologyId(command);

      MapResponseCollector collector =
         MapResponseCollector.ignoreLeavers(shouldIgnoreLeavers(rpcOptions), targets.size());

      if (rpcOptions.responseMode().isSynchronous()) {
         return invokeCommand(targets, command, collector, rpcOptions).toCompletableFuture();
      } else {
         sendToMany(recipients, command, rpcOptions.deliverOrder());
         return CompletableFutures.completedEmptyMap();
      }
   }

   @Deprecated
   @Override
   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand command,
                                                      RpcOptions rpcOptions) {
      return realOne.blocking(invokeRemotelyAsync(recipients, command, rpcOptions));
   }

   private void setTopologyId(ReplicableCommand command) {
      if (command instanceof TopologyAffectedCommand && ((TopologyAffectedCommand) command).getTopologyId() < 0) {
         ((TopologyAffectedCommand) command).setTopologyId(getTopologyId());
      }
   }

   @Deprecated
   private boolean shouldIgnoreLeavers(RpcOptions rpcOptions) {
      return rpcOptions.responseMode() != ResponseMode.SYNCHRONOUS;
   }

   @Override
   public final Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcs, RpcOptions options) {
      try {
         rpcs.forEach((address, command) -> setTopologyId(command));
         return CompletableFutures.await(
            invokeCommands(rpcs.keySet(), rpcs::get, MapResponseCollector.validOnly(rpcs.size()), options)
               .toCompletableFuture());
      } catch (ExecutionException | InterruptedException e) {
         throw Util.rewrapAsCacheException(e);
      }
   }

   @Override
   public final void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder) {
      setTopologyId(command);
      performSend(Collections.singleton(destination), command,
                  c -> {
                     realOne.sendTo(destination, command, deliverOrder);
                     return null;
                  });
   }

   @Override
   public final void sendToMany(Collection<Address> destinations, ReplicableCommand command,
                                DeliverOrder deliverOrder) {
      setTopologyId(command);
      performSend(destinations, command,
                  c -> {
                     realOne.sendToMany(destinations, command, deliverOrder);
                     return null;
                  });
   }

   @Override
   public final void sendToAll(ReplicableCommand command, DeliverOrder deliverOrder) {
      setTopologyId(command);
      performSend(getTransport().getMembers(), command,
                  c -> {
                     realOne.sendToAll(command, deliverOrder);
                     return null;
                  });
   }

   @Override
   public BackupResponse invokeXSite(Collection<XSiteBackup> sites, XSiteReplicateCommand command) throws Exception {
      return realOne.invokeXSite(sites, command);
   }

   @Override
   public XSiteResponse invokeXSite(XSiteBackup backup, XSiteReplicateCommand command) {
      return realOne.invokeXSite(backup, command);
   }

   @Override
   public Transport getTransport() {
      return realOne.getTransport();
   }

   @Override
   public List<Address> getMembers() {
      return realOne.getMembers();
   }

   @Override
   public Address getAddress() {
      return realOne.getAddress();
   }

   @Override
   public int getTopologyId() {
      return realOne.getTopologyId();
   }

   @Override
   public RpcOptions getSyncRpcOptions() {
      return realOne.getSyncRpcOptions();
   }

   @Override
   public RpcOptions getTotalSyncRpcOptions() {
      return realOne.getTotalSyncRpcOptions();
   }

   @Deprecated
   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode) {
      return realOne.getRpcOptionsBuilder(responseMode);
   }

   @Deprecated
   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, DeliverOrder deliverOrder) {
      return realOne.getRpcOptionsBuilder(responseMode, deliverOrder);
   }

   @Deprecated
   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync) {
      return realOne.getDefaultRpcOptions(sync);
   }

   @Deprecated
   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync, DeliverOrder deliverOrder) {
      return realOne.getDefaultRpcOptions(sync, deliverOrder);
   }

   /**
    * Wrap the remote invocation.
    */
   protected <T> CompletionStage<T> performRequest(Collection<Address> targets, ReplicableCommand command,
                                                   ResponseCollector<T> collector,
                                                   Function<ResponseCollector<T>, CompletionStage<T>> invoker,
                                                   RpcOptions rpcOptions) {
      return invoker.apply(collector);
   }


   /**
    * Wrap the remote invocation.
    */
   protected <T> void performSend(Collection<Address> targets, ReplicableCommand command,
                                  Function<ResponseCollector<T>, CompletionStage<T>> invoker) {
      invoker.apply(null);
   }

   private static class CommandsRequest<T> implements BiConsumer<Map<Address, Response>, Throwable> {
      private final Collection<Address> targets;
      private final ResponseCollector<T> collector;
      CompletableFuture<T> resultFuture;
      int missingResponses;

      public CommandsRequest(Collection<Address> targets, ResponseCollector<T> collector) {
         this.targets = targets;
         this.collector = collector;
         resultFuture = new CompletableFuture<>();
         missingResponses = targets.size();
      }

      @Override
      public void accept(Map<Address, Response> responseMap, Throwable throwable) {
         T result;
         boolean finish;
         synchronized (this) {
            missingResponses--;
            if (resultFuture.isDone()) {
               return;
            }
            try {
               if (responseMap == null) {
                  // A request to the local node don't get any response in non-total order caches
                  return;
               }
               Map.Entry<Address, Response> singleResponse = responseMap.entrySet().iterator().next();
               result = collector.addResponse(singleResponse.getKey(), singleResponse.getValue());
            } catch (Throwable t) {
               resultFuture.completeExceptionally(t);
               throw t;
            }
            finish = missingResponses == 0;
         }
         if (result != null) {
            resultFuture.complete(result);
         } else if (finish) {
            try {
               resultFuture.complete(collector.finish());
            } catch (Throwable t) {
               resultFuture.completeExceptionally(t);
               throw t;
            }
         }
      }
   }
}
