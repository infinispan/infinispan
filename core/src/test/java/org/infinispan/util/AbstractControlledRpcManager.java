package org.infinispan.util;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Common rpc manager controls
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class AbstractControlledRpcManager implements RpcManager {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   protected final RpcManager realOne;

   public AbstractControlledRpcManager(RpcManager realOne) {
      this.realOne = realOne;
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                               ResponseCollector<T> collector, RpcOptions rpcOptions) {
      log.trace("ControlledRpcManager.invokeCommand");
      Object argument = beforeInvokeRemotely(command);
      CompletionStage<T> future = realOne.invokeCommand(target, command, collector, rpcOptions);
      return future.thenApply(response -> afterInvokeRemotely(command, response, argument));
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                               ResponseCollector<T> collector, RpcOptions rpcOptions) {
      log.trace("ControlledRpcManager.invokeCommand");
      Object argument = beforeInvokeRemotely(command);
      CompletionStage<T> future = realOne.invokeCommand(targets, command, collector, rpcOptions);
      return future.thenApply(response -> afterInvokeRemotely(command, response, argument));
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                    RpcOptions rpcOptions) {
      log.trace("ControlledRpcManager.invokeCommandOnAll");
      Object argument = beforeInvokeRemotely(command);
      CompletionStage<T> future = realOne.invokeCommandOnAll(command, collector, rpcOptions);
      return future.thenApply(response -> afterInvokeRemotely(command, response, argument));
   }

   @Override
   public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                        ResponseCollector<T> collector, RpcOptions rpcOptions) {
      log.trace("ControlledRpcManager.invokeCommandStaggered");
      Object argument = beforeInvokeRemotely(command);
      CompletionStage<T> future = realOne.invokeCommandStaggered(targets, command, collector, rpcOptions);
      return future.thenApply(response -> afterInvokeRemotely(command, response, argument));
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                Function<Address, ReplicableCommand> commandGenerator,
                                                ResponseCollector<T> collector, RpcOptions rpcOptions) {
      // TODO: left blank until we need to implement
      return realOne.invokeCommands(targets, commandGenerator, collector, rpcOptions);
   }

   @Override
   public <T> T blocking(CompletionStage<T> request) {
      return realOne.blocking(request);
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpc,
                                                                        RpcOptions options) {
      log.trace("ControlledRpcManager.invokeRemotelyAsync");
      Object argument = beforeInvokeRemotely(rpc);
      CompletableFuture<Map<Address, Response>> future = realOne.invokeRemotelyAsync(recipients, rpc,
            options);
      return future.thenApply(responses -> afterInvokeRemotely(rpc, responses, argument));
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      log.trace("ControlledRpcManager.invokeRemotely");
      Object argument = beforeInvokeRemotely(rpc);
      Map<Address, Response> responses = realOne.invokeRemotely(recipients, rpc, options);
      return afterInvokeRemotely(rpc, responses, argument);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcs, RpcOptions options) {
      log.trace("ControlledRpcManager.invokeRemotely");
      // TODO: left blank until we need to implement
      return realOne.invokeRemotely(rpcs, options);
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder) {
      realOne.sendTo(destination, command, deliverOrder);
   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand command, DeliverOrder deliverOrder) {
      realOne.sendToMany(destinations, command, deliverOrder);
   }

   @Override
   public void sendToAll(ReplicableCommand command, DeliverOrder deliverOrder) {
      realOne.sendToAll(command, deliverOrder);
   }

   @Override
   public Transport getTransport() {
      return realOne.getTransport();
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
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode) {
      return realOne.getRpcOptionsBuilder(responseMode);
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, DeliverOrder deliverOrder) {
      return realOne.getRpcOptionsBuilder(responseMode, deliverOrder);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync) {
      return realOne.getDefaultRpcOptions(sync);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync, DeliverOrder deliverOrder) {
      return realOne.getDefaultRpcOptions(sync, deliverOrder);
   }

   @Override
   public RpcOptions getSyncRpcOptions() {
      return realOne.getSyncRpcOptions();
   }

   @Override
   public RpcOptions getTotalSyncRpcOptions() {
      return realOne.getTotalSyncRpcOptions();
   }

   @Override
   public List<Address> getMembers() {
      return realOne.getMembers();
   }

   /**
    * method invoked before a remote invocation.
    *
    * @param command the command to be invoked remotely
    */
   protected Object beforeInvokeRemotely(ReplicableCommand command) {
      //no-op by default
      return null;
   }

   /**
    * method invoked after a successful remote invocation.
    *
    * @param command     the command invoked remotely.
    * @param responseObject can be null if not response is expected.
    * @param argument
    * @return the new response map
    */
   protected <T> T afterInvokeRemotely(ReplicableCommand command, T responseObject, Object argument) {
      return (T) responseObject;
   }
}
