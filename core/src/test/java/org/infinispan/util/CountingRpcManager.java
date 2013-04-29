/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

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
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
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

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      log.trace("invokeRemotely1");
      aboutToInvokeRpc(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      log.trace("invokeRemotely2");
      aboutToInvokeRpc(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) {
      log.trace("invokeRemotely3");
      aboutToInvokeRpc(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout);
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws RpcException {
      log.trace("invokeRemotely4");
      aboutToInvokeRpc(rpc);
      realOne.invokeRemotely(recipients, rpc, sync);
      return null;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("invokeRemotely5");
      aboutToInvokeRpc(rpc);
      Map<Address, Response> responses = realOne.invokeRemotely(recipients, rpc, sync, usePriorityQueue);
      return responses;
   }


   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand1");
      aboutToInvokeRpc(rpc);
      realOne.broadcastRpcCommand(rpc, sync);
   }

   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand2");
      aboutToInvokeRpc(rpc);
      realOne.broadcastRpcCommand(rpc, sync, usePriorityQueue);
   }


   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture1");
      aboutToInvokeRpc(rpc);
      realOne.broadcastRpcCommandInFuture(rpc, future);
   }

   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture2");
      aboutToInvokeRpc(rpc);
      realOne.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
   }


   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture1");
      aboutToInvokeRpc(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, future);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture2");
      aboutToInvokeRpc(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
   }

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

   public Transport getTransport() {
      return realOne.getTransport();
   }

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
