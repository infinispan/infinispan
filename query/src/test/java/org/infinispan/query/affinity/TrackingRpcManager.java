package org.infinispan.query.affinity;

import java.util.Collection;
import java.util.Collections;
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
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

class TrackingRpcManager implements RpcManager {

   private final RpcManager delegate;
   private final RpcCollector rpcCollector;
   private final String cacheName;

   TrackingRpcManager(RpcManager delegate, RpcCollector rpcCollector, String cacheName) {
      this.delegate = delegate;
      this.rpcCollector = rpcCollector;
      this.cacheName = cacheName;
   }


   @Override
   public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                               ResponseCollector<T> collector, RpcOptions rpcOptions) {
      rpcCollector.addRPC(new RpcDetail(getAddress(), command, cacheName, Collections.singleton(target)));
      return delegate.invokeCommand(target, command, collector, rpcOptions);
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                               ResponseCollector<T> collector, RpcOptions rpcOptions) {
      rpcCollector.addRPC(new RpcDetail(getAddress(), command, cacheName, targets));
      return delegate.invokeCommand(targets, command, collector, rpcOptions);
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                    RpcOptions rpcOptions) {
      rpcCollector.addRPC(new RpcDetail(getAddress(), command, cacheName, delegate.getTransport().getMembers()));
      return delegate.invokeCommandOnAll(command, collector, rpcOptions);
   }

   @Override
   public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                        ResponseCollector<T> collector, RpcOptions rpcOptions) {
      rpcCollector.addRPC(new RpcDetail(getAddress(), command, cacheName, targets));
      return delegate.invokeCommandStaggered(targets, command, collector, rpcOptions);
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                Function<Address, ReplicableCommand> commandGenerator,
                                                ResponseCollector<T> collector, RpcOptions rpcOptions) {
      targets.forEach(target -> rpcCollector.addRPC(
            new RpcDetail(getAddress(), commandGenerator.apply(target), cacheName, Collections.singleton(target))));
      return delegate.invokeCommands(targets, commandGenerator, collector, rpcOptions);
   }

   @Override
   public <T> T blocking(CompletionStage<T> request) {
      return delegate.blocking(request);
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      rpcCollector.addRPC(new RpcDetail(getAddress(), rpc, cacheName, recipients));
      return delegate.invokeRemotelyAsync(recipients, rpc, options);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      rpcCollector.addRPC(new RpcDetail(getAddress(), rpc, cacheName, recipients));
      return delegate.invokeRemotely(recipients, rpc, options);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcs, RpcOptions options) {
      rpcs.forEach((key, value) -> rpcCollector
            .addRPC(new RpcDetail(getAddress(), value, cacheName, Collections.singleton(key))));
      return delegate.invokeRemotely(rpcs, options);
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder) {
      rpcCollector.addRPC(new RpcDetail(getAddress(), command, cacheName, Collections.singletonList(destination)));
      delegate.sendTo(destination, command, deliverOrder);
   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand command, DeliverOrder deliverOrder) {
      rpcCollector.addRPC(new RpcDetail(getAddress(), command, cacheName, destinations));
      delegate.sendToMany(destinations, command, deliverOrder);
   }

   @Override
   public void sendToAll(ReplicableCommand command, DeliverOrder deliverOrder) {
      rpcCollector.addRPC(new RpcDetail(getAddress(), command, cacheName, delegate.getMembers()));
      delegate.sendToAll(command, deliverOrder);
   }

   @Override
   public BackupResponse invokeXSite(Collection<XSiteBackup> sites, XSiteReplicateCommand command) throws Exception {
      return delegate.invokeXSite(sites, command);
   }

   @Override
   public Transport getTransport() {
      return delegate.getTransport();
   }

   @Override
   public List<Address> getMembers() {
      return delegate.getMembers();
   }

   @Override
   public Address getAddress() {
      return delegate.getAddress();
   }

   @Override
   public int getTopologyId() {
      return delegate.getTopologyId();
   }

   @Override
   public RpcOptions getSyncRpcOptions() {
      return delegate.getSyncRpcOptions();
   }

   @Override
   public RpcOptions getTotalSyncRpcOptions() {
      return delegate.getTotalSyncRpcOptions();
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode) {
      return delegate.getRpcOptionsBuilder(responseMode);
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, DeliverOrder deliverOrder) {
      return delegate.getRpcOptionsBuilder(responseMode, deliverOrder);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync) {
      return delegate.getDefaultRpcOptions(sync);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync, DeliverOrder deliverOrder) {
      return delegate.getDefaultRpcOptions(sync, deliverOrder);
   }
}
