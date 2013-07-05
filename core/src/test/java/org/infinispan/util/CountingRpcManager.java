package org.infinispan.util;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.commons.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
* Use the {@link CountingRpcManager#replaceRpcManager(org.infinispan.Cache)}.
* @author Mircea Markus
* @since 5.2
*/
public class CountingRpcManager implements RpcManager {

   private static final Log log = LogFactory.getLog(CountingRpcManager.class);

   public volatile int lockCount;
   public volatile int clusterGet;
   public volatile int otherCount;

   protected final RpcManager realOne;


   public static CountingRpcManager replaceRpcManager(Cache c) {
      AdvancedCache advancedCache = c.getAdvancedCache();
      CountingRpcManager crm = new CountingRpcManager(advancedCache.getRpcManager());
      advancedCache.getComponentRegistry().registerComponent(crm, RpcManager.class);
      advancedCache.getComponentRegistry().rewire();
      assert advancedCache.getRpcManager().equals(crm);
      return crm;
   }

   public CountingRpcManager(RpcManager realOne) {
      this.realOne = realOne;
   }

   protected void aboutToInvokeRpc(ReplicableCommand rpcCommand) {
      if (rpcCommand instanceof LockControlCommand) {
         lockCount++;
      } else if (rpcCommand instanceof ClusteredGetCommand) {
         clusterGet++;
      } else {
         otherCount++;
      }
   }

   public void resetStats() {
      lockCount = 0;
      clusterGet = 0;
      otherCount = 0;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      log.trace("invokeRemotely1");
      aboutToInvokeRpc(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      log.trace("invokeRemotely2");
      aboutToInvokeRpc(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) {
      log.trace("invokeRemotely3");
      aboutToInvokeRpc(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws RpcException {
      log.trace("invokeRemotely4");
      aboutToInvokeRpc(rpc);
      realOne.invokeRemotely(recipients, rpc, sync);
      return null;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("invokeRemotely5");
      aboutToInvokeRpc(rpc);
      Map<Address, Response> responses = realOne.invokeRemotely(recipients, rpc, sync, usePriorityQueue);
      return responses;
   }


   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand1");
      aboutToInvokeRpc(rpc);
      realOne.broadcastRpcCommand(rpc, sync);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand2");
      aboutToInvokeRpc(rpc);
      realOne.broadcastRpcCommand(rpc, sync, usePriorityQueue);
   }


   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture1");
      aboutToInvokeRpc(rpc);
      realOne.broadcastRpcCommandInFuture(rpc, future);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture2");
      aboutToInvokeRpc(rpc);
      realOne.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
   }


   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture1");
      aboutToInvokeRpc(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture2");
      aboutToInvokeRpc(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture3");
      aboutToInvokeRpc(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture4");
      aboutToInvokeRpc(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout, ignoreLeavers);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      log.trace("CountingRpcManager.invokeRemotely6");
      aboutToInvokeRpc(rpc);
      return realOne.invokeRemotely(recipients, rpc, options);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options, NotifyingNotifiableFuture<Object> future) {
      log.trace("CountingRpcManager.invokeRemotelyInFuture5");
      aboutToInvokeRpc(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, options, future);
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
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, boolean fifoOrder) {
      return realOne.getRpcOptionsBuilder(responseMode, fifoOrder);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync) {
      return realOne.getDefaultRpcOptions(sync);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync, boolean fifoOrder) {
      return realOne.getDefaultRpcOptions(sync, fifoOrder);
   }

   @Override
   public List<Address> getMembers() {
      return realOne.getMembers();
   }
}
