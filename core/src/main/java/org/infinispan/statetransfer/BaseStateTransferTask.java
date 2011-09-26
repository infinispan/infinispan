/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.statetransfer;

import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Push state from the existing members of the cluster to the new members.
 * This is a base class, extended by the distributed an replicated versions.
 */
public abstract class BaseStateTransferTask {
   private static final Log log = LogFactory.getLog(BaseStateTransferTask.class);

   protected final Configuration configuration;
   protected final BaseStateTransferManagerImpl stateTransferManager;
   protected final StateTransferLock stateTransferLock;
   protected final CacheNotifier cacheNotifier;
   protected final int newViewId;
   protected final DataContainer dataContainer;
   protected final Address self;
   protected final boolean trace = log.isTraceEnabled();
   protected final Collection<Address> members;
   protected final ConsistentHash chOld;
   protected final ConsistentHash chNew;
   protected final boolean initialView;

   public BaseStateTransferTask(BaseStateTransferManagerImpl stateTransferManager, RpcManager rpcManager, StateTransferLock stateTransferLock, CacheNotifier cacheNotifier, Configuration configuration, DataContainer dataContainer, Collection<Address> members, int newViewId, ConsistentHash chNew, ConsistentHash chOld, boolean initialView) {
      this.stateTransferLock = stateTransferLock;
      this.initialView = initialView;
      this.stateTransferManager = stateTransferManager;
      this.cacheNotifier = cacheNotifier;
      this.self = rpcManager.getAddress();
      this.configuration = configuration;
      this.members = members;
      this.newViewId = newViewId;
      this.dataContainer = dataContainer;
      this.chNew = chNew;
      this.chOld = chOld;
   }

   protected abstract void performStateTransfer() throws Exception;

   protected abstract void commitStateTransfer();

   protected void pushState(Map<Address, Collection<InternalCacheEntry>> states)
         throws InterruptedException, ExecutionException, PendingStateTransferException, TimeoutException {
      NotifyingNotifiableFuture<Object> stateTransferFuture = new AggregatingNotifyingFutureImpl(null, states.size());
      for (Map.Entry<Address, Collection<InternalCacheEntry>> entry : states.entrySet()) {
         final Address target = entry.getKey();
         if (stateTransferManager.getLeavers().contains(target)) {
            log.debugf("Not sending state to node %s, it has already left the cluster", target);
            continue;
         }
         Collection<InternalCacheEntry> state = entry.getValue();
         stateTransferManager.pushStateToNode(stateTransferFuture, newViewId, target, state);
      }

      // wait to see if all servers received the new state
      stateTransferFuture.get(configuration.getRehashRpcTimeout(), TimeUnit.MILLISECONDS);
      log.debugf("Node finished pushing data for rehash %d.", newViewId);
   }
}
