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
import org.infinispan.commands.remote.SingleRpcCommand;
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
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class ControlledRpcManager implements RpcManager {

   private static final Log log = LogFactory.getLog(ControlledRpcManager.class);

   private final ReclosableLatch replicationLatch = new ReclosableLatch(true);
   private final ReclosableLatch blockingLatch = new ReclosableLatch(true);
   private volatile Set<Class> blockBeforeFilter = Collections.emptySet();
   private volatile Set<Class> blockAfterFilter = Collections.emptySet();

   private volatile Set<Class> failFilter = Collections.emptySet();

   protected RpcManager realOne;

   public ControlledRpcManager(RpcManager realOne) {
      this.realOne = realOne;
   }

   public void failFor(Class... filter) {
      this.failFilter = new HashSet<Class>(Arrays.asList(filter));
   }

   public void stopFailing() {
      this.failFilter = Collections.emptySet();
   }

   public void blockBefore(Class... filter) {
      this.blockBeforeFilter = new HashSet<Class>(Arrays.asList(filter));
      replicationLatch.close();
   }

   public void blockAfter(Class... filter) {
      this.blockAfterFilter = new HashSet<Class>(Arrays.asList(filter));
      replicationLatch.close();
   }

   public void stopBlocking() {
      blockBeforeFilter = Collections.emptySet();
      blockAfterFilter = Collections.emptySet();
      replicationLatch.open();
   }

   public void waitForCommandToBlock() throws InterruptedException {
      blockingLatch.await();
   }

   protected void waitBefore(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockBeforeFilter);
   }

   protected void waitAfter(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockAfterFilter);
   }

   protected void waitForReplicationLatch(ReplicableCommand rpcCommand, Set<Class> filter) {
      Class cmdClass = getActualClass(rpcCommand);
      if (!filter.contains(cmdClass)) {
         return;
      }

      try {
         log.debugf("Replication trigger called, waiting for latch to open.");
         replicationLatch.await();
         log.trace("Replication latch opened, continuing.");
      } catch (Exception e) {
         throw new RuntimeException("Unexpected exception!", e);
      }
   }

   private Class getActualClass(ReplicableCommand rpcCommand) {
      Class cmdClass = rpcCommand.getClass();
      if (cmdClass.equals(SingleRpcCommand.class)) {
         cmdClass = ((SingleRpcCommand) rpcCommand).getCommand().getClass();
      }
      return cmdClass;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      log.trace("invokeRemotely1");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
      waitAfter(rpcCommand);
      return responseMap;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      log.trace("invokeRemotely2");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
      waitAfter(rpcCommand);
      return responseMap;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) {
      log.trace("invokeRemotely3");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, mode, timeout);
      waitAfter(rpcCommand);
      return responseMap;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean sync) throws RpcException {
      log.trace("invokeRemotely4");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, sync);
      waitAfter(rpcCommand);
      return responseMap;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("invokeRemotely5");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responses = realOne.invokeRemotely(recipients, rpcCommand, sync, usePriorityQueue);
      waitAfter(rpcCommand);
      return responses;
   }

   public void broadcastRpcCommand(ReplicableCommand rpcCommand, boolean sync) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand1");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.broadcastRpcCommand(rpcCommand, sync);
      waitAfter(rpcCommand);
   }

   public void broadcastRpcCommand(ReplicableCommand rpcCommand, boolean sync, boolean usePriorityQueue) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand2");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.broadcastRpcCommand(rpcCommand, sync, usePriorityQueue);
      waitAfter(rpcCommand);
   }

   public void broadcastRpcCommandInFuture(ReplicableCommand rpcCommand, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture1");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.broadcastRpcCommandInFuture(rpcCommand, future);
      waitAfter(rpcCommand);
   }

   public void broadcastRpcCommandInFuture(ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture2");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.broadcastRpcCommandInFuture(rpcCommand, usePriorityQueue, future);
      waitAfter(rpcCommand);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture1");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, future);
      waitAfter(rpcCommand);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture2");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future);
      waitAfter(rpcCommand);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture3");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future, timeout);
      waitAfter(rpcCommand);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture4");
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future, timeout, ignoreLeavers);
      waitAfter(rpcCommand);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      log.trace("ControlledRpcManager.invokeRemotely6");
      failIfNeeded(rpc);
      waitBefore(rpc);
      Map<Address, Response> responses = realOne.invokeRemotely(recipients, rpc, options);
      waitAfter(rpc);
      return responses;
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture5");
      failIfNeeded(rpc);
      waitBefore(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, options, future);
      waitAfter(rpc);
   }

   public Transport getTransport() {
      return realOne.getTransport();
   }

   public Address getAddress() {
      return realOne.getAddress();
   }

   public void failIfNeeded(ReplicableCommand rpcCommand) {
      if (failFilter.contains(getActualClass(rpcCommand))) {
         throw new IllegalStateException("Induced failure!");
      }
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
