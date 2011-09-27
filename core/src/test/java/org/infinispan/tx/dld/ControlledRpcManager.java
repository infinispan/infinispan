/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.tx.dld;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
* @author Mircea.Markus@jboss.com
* @since 4.2
*/
   public class ControlledRpcManager implements RpcManager {

   private static final Log log = LogFactory.getLog(ControlledRpcManager.class);

   private volatile CountDownLatch replicationLatch;

   private boolean fail;

   public ControlledRpcManager(RpcManager realOne) {
      this(realOne, new CountDownLatch(1));
      replicationLatch.countDown();
   }

   public ControlledRpcManager(RpcManager realOne, CountDownLatch latch) {
      this.realOne = realOne;
      replicationLatch = latch;
   }


   protected RpcManager realOne;

   public boolean isFail() {
      return fail;
   }

   public void setFail(boolean fail) {
      this.fail = fail;
   }

   public void setReplicationLatch(CountDownLatch replicationLatch) {
      this.replicationLatch = replicationLatch;
   }

   protected void waitFirst(ReplicableCommand rpcCommand) {
      failIfNeeded();
      boolean isLockControlCommand = rpcCommand instanceof LockControlCommand;
      boolean isClusterGet = rpcCommand instanceof ClusteredGetCommand;
      if (!isClusterGet && !isLockControlCommand) {
         log.debugf("%s -- replication trigger called!", Thread.currentThread().getName());
         waitForLatchToOpen();
      }
   }

   protected void waitForLatchToOpen() {
      try {
         replicationLatch.await();
         log.trace("Replication latch opened, continuing...");
      } catch (Exception e) {
         throw new RuntimeException("Unexpected exception!", e);
      }
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      log.trace("invokeRemotely1");
      waitFirst(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      log.trace("invokeRemotely2");
      waitFirst(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) {
      log.trace("invokeRemotely3");
      waitFirst(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout);
   }

   public void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws RpcException {
      log.trace("invokeRemotely4");
      waitFirst(rpc);
      realOne.invokeRemotely(recipients, rpc, sync);
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("invokeRemotely5");
      Map<Address, Response> responses = realOne.invokeRemotely(recipients, rpc, sync, usePriorityQueue);
      waitForLatchToOpen();
      return responses;
   }


   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand1");
      waitFirst(rpc);
      realOne.broadcastRpcCommand(rpc, sync);
   }

   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand2");
      failIfNeeded();
      realOne.broadcastRpcCommand(rpc, sync, usePriorityQueue);
      waitForLatchToOpen();
   }


   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture1");
      waitFirst(rpc);
      realOne.broadcastRpcCommandInFuture(rpc, future);
   }

   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture2");
      waitFirst(rpc);
      realOne.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
   }


   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture1");
      waitFirst(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, future);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture2");
      waitFirst(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture3");
      waitFirst(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
   }

   public Transport getTransport() {
      return realOne.getTransport();
   }

   public Address getAddress() {
      return realOne.getAddress();
   }

   public void failIfNeeded() {
      if (fail) throw new IllegalStateException("Induced failure!");
   }
}
