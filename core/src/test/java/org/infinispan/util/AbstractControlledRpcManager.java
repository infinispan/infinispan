package org.infinispan.util;

import org.infinispan.commands.ReplicableCommand;
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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Common rpc manager controls
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class AbstractControlledRpcManager implements RpcManager {

   protected final Log log = LogFactory.getLog(getClass());
   protected final RpcManager realOne;

   public AbstractControlledRpcManager(RpcManager realOne) {
      this.realOne = realOne;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      log.trace("ControlledRpcManager.invokeRemotely1");
      beforeInvokeRemotely(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
      return afterInvokeRemotely(rpcCommand, responseMap);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      log.trace("ControlledRpcManager.invokeRemotely2");
      beforeInvokeRemotely(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
      return afterInvokeRemotely(rpcCommand, responseMap);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) {
      log.trace("ControlledRpcManager.invokeRemotely3");
      beforeInvokeRemotely(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, mode, timeout);
      return afterInvokeRemotely(rpcCommand, responseMap);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean sync) throws RpcException {
      log.trace("ControlledRpcManager.invokeRemotely4");
      beforeInvokeRemotely(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, sync);
      return afterInvokeRemotely(rpcCommand, responseMap);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("ControlledRpcManager.invokeRemotely5");
      beforeInvokeRemotely(rpcCommand);
      Map<Address, Response> responses = realOne.invokeRemotely(recipients, rpcCommand, sync, usePriorityQueue);
      return afterInvokeRemotely(rpcCommand, responses);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpcCommand, boolean sync) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand1");
      beforeInvokeRemotely(rpcCommand);
      realOne.broadcastRpcCommand(rpcCommand, sync);
      afterInvokeRemotely(rpcCommand, null);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpcCommand, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand2");
      beforeInvokeRemotely(rpcCommand);
      realOne.broadcastRpcCommand(rpcCommand, sync, usePriorityQueue);
      afterInvokeRemotely(rpcCommand, null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpcCommand, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture1");
      beforeInvokeRemotely(rpcCommand);
      realOne.broadcastRpcCommandInFuture(rpcCommand, future);
      afterInvokeRemotely(rpcCommand, null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture2");
      beforeInvokeRemotely(rpcCommand);
      realOne.broadcastRpcCommandInFuture(rpcCommand, usePriorityQueue, future);
      afterInvokeRemotely(rpcCommand, null);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture1");
      beforeInvokeRemotely(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, future);
      afterInvokeRemotely(rpcCommand, null);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture2");
      beforeInvokeRemotely(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future);
      afterInvokeRemotely(rpcCommand, null);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture3");
      beforeInvokeRemotely(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future, timeout);
      afterInvokeRemotely(rpcCommand, null);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture4");
      beforeInvokeRemotely(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future, timeout, ignoreLeavers);
      afterInvokeRemotely(rpcCommand, null);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      log.trace("ControlledRpcManager.invokeRemotely6");
      beforeInvokeRemotely(rpc);
      Map<Address, Response> responses = realOne.invokeRemotely(recipients, rpc, options);
      return afterInvokeRemotely(rpc, responses);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture5");
      beforeInvokeRemotely(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, options, future);
      afterInvokeRemotely(rpc, null);
   }

   @Override
   public void invokeRemotelyInFuture(NotifyingNotifiableFuture<Map<Address, Response>> future, Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture6");
      beforeInvokeRemotely(rpc);
      realOne.invokeRemotelyInFuture(future, recipients, rpc, options);
      afterInvokeRemotely(rpc, null);
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

   /**
    * method invoked before a remote invocation.
    *
    * @param command the command to be invoked remotely
    */
   protected void beforeInvokeRemotely(ReplicableCommand command) {
      //no-op by default
   }

   /**
    * method invoked after a successful remote invocation.
    *
    * @param command     the command invoked remotely.
    * @param responseMap can be null if not response is expected.
    * @return the new response map
    */
   protected Map<Address, Response> afterInvokeRemotely(ReplicableCommand command, Map<Address, Response> responseMap) {
      return responseMap;
   }
}
