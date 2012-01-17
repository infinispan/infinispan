/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.lock.singlelock;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import java.util.Collection;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.singlelock.SingleRpcOnPessimisticLockingTest")
public class SingleRpcOnPessimisticLockingTest extends MultipleCacheManagersTest {

   private Object k0;
   private CountingRpcManager crm;

   @Override
   protected void createCacheManagers() throws Throwable {
      final Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      c.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      c.fluent().hash().numOwners(1);
      c.fluent().l1().disable();
      createCluster(c, 2);
      waitForClusterToForm();

      k0 = getKeyForCache(1);
      crm = new CountingRpcManager(advancedCache(0).getRpcManager());
      advancedCache(0).getComponentRegistry().registerComponent(crm, RpcManager.class);
      advancedCache(0).getComponentRegistry().rewire();
      assert advancedCache(0).getRpcManager().equals(crm);
   }

   @BeforeMethod
   void clearStats() {
      crm.resetStats();
   }

   public void testSingleGetOnPut() throws Exception {

      Operation o = new Operation() {
         @Override
         public void execute() {
            cache(0).put(k0, "v0");
         }
      };

      runtTest(o);
   }

   public void testSingleGetOnRemove() throws Exception {

      Operation o = new Operation() {
         @Override
         public void execute() {
            cache(0).remove(k0);
         }
      };

      runtTest(o);
   }

   private void runtTest(Operation o) throws NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
      log.trace("Here is where the fun starts..");
      tm(0).begin();

      o.execute();

      assertKeyLockedCorrectly(k0);

      assertEquals(crm.lockCount, 0);
      assertEquals(crm.clusterGet, 1);
      assertEquals(crm.otherCount, 0);

      tm(0).commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return crm.lockCount == 0 && crm.clusterGet == 1 &&
                  crm.otherCount == 1;//1-phase commit
         }
      });
   }

   public static class CountingRpcManager implements RpcManager {

      private static final Log log = LogFactory.getLog(CountingRpcManager.class);

      public volatile int lockCount;
      public volatile int clusterGet;
      public volatile int otherCount;

      protected final RpcManager realOne;

      public CountingRpcManager(RpcManager realOne) {
         this.realOne = realOne;
      }

      protected void aboutToInvokeRpc(ReplicableCommand rpcCommand) {
         System.out.println("rpcCommand = " + rpcCommand);
         if (rpcCommand instanceof LockControlCommand) {
            lockCount++;
         } else if (rpcCommand instanceof ClusteredGetCommand) {
            clusterGet++;
         } else {
            otherCount++;
         }
      }

      void resetStats() {
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

      public void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws RpcException {
         log.trace("invokeRemotely4");
         aboutToInvokeRpc(rpc);
         realOne.invokeRemotely(recipients, rpc, sync);
      }

      public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
         log.trace("invokeRemotely5");
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

      public Transport getTransport() {
         return realOne.getTransport();
      }

      public Address getAddress() {
         return realOne.getAddress();
      }
   }

   private interface Operation {
      void execute();
   }
}
