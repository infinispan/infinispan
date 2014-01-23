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
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Use the {@link CountingRpcManager#replaceRpcManager(org.infinispan.Cache)}.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class CountingRpcManager extends AbstractControlledRpcManager {

   public volatile int lockCount;
   public volatile int clusterGet;
   public volatile int otherCount;

   public CountingRpcManager(RpcManager realOne) {
      super(realOne);
   }

   public static CountingRpcManager replaceRpcManager(Cache c) {
      AdvancedCache advancedCache = c.getAdvancedCache();
      CountingRpcManager crm = new CountingRpcManager(advancedCache.getRpcManager());
      advancedCache.getComponentRegistry().registerComponent(crm, RpcManager.class);
      advancedCache.getComponentRegistry().rewire();
      assert advancedCache.getRpcManager().equals(crm);
      return crm;
   }

   public void resetStats() {
      lockCount = 0;
      clusterGet = 0;
      otherCount = 0;
   }

   @Override
   protected void beforeInvokeRemotely(ReplicableCommand rpcCommand) {
      if (rpcCommand instanceof LockControlCommand) {
         lockCount++;
      } else if (rpcCommand instanceof ClusteredGetCommand) {
         clusterGet++;
      } else {
         otherCount++;
      }
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
   public List<Address> getMembers() {
      return realOne.getMembers();
   }
}
