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
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.MembershipArithmetic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Task which pushes keys to new nodes during join.
 * <p/>To reduce the duration of the state transfer, all the existing members participate in the state transfer.
 * <p/>We use a <code>ConsistentHash</code> to decide which existing node is supposed to push a certain key.
 * <p/> Example:<p/>
 * <pre>
 * - The membership is {A,B,C,D}
 * - The new view is {A,B,C,D,E,F}
 * - For K, the old CH is A,B and the new CH is A,C
 * - A (since it is K's owner) now pushes K to C
 * - For K2, the old CH is A,B and the new CH is B,C
 * - B (since it is the backup owner and A left) pushes K2 to C
 * </pre>
 *
 * @author Bela Ban
 * @author Dan Berindei <dan@infinispan.org>
 * @since 4.2
 */
public class ReplicatedStateTransferTask extends BaseStateTransferTask {
   private static final Log log = LogFactory.getLog(ReplicatedStateTransferTask.class);

   private final ReplicatedStateTransferManagerImpl stateTransferManager;
   private long stateTransferStartMillis;

   public ReplicatedStateTransferTask(RpcManager rpcManager, Configuration configuration, DataContainer dataContainer,
                                      ReplicatedStateTransferManagerImpl stateTransferManager, StateTransferLock stateTransferLock,
                                      CacheNotifier cacheNotifier, int newViewId, Collection<Address> members,
                                      ConsistentHash chOld, ConsistentHash chNew, boolean initialView) {
      super(stateTransferManager, rpcManager, stateTransferLock, cacheNotifier, configuration, dataContainer, members, newViewId, chNew, chOld, initialView);
      this.stateTransferManager = stateTransferManager;
   }


   @Override
   public void doPerformStateTransfer() throws Exception {
      if (!stateTransferManager.startStateTransfer(newViewId, members, initialView))
         return;

      if (log.isDebugEnabled())
         log.debugf("Commencing state transfer %d on node: %s. Before start, data container had %d entries",
               newViewId, self, dataContainer.size());

      // Don't need to log anything, all transactions will be blocked
      //distributionManager.getTransactionLogger().enable();
      stateTransferLock.blockNewTransactions(newViewId);

      Set<Address> joiners = chOld != null ? MembershipArithmetic.getMembersJoined(chOld.getCaches(), chNew.getCaches()) : chNew.getCaches();
      if (joiners.isEmpty()) {
         log.tracef("No joiners in view %s, skipping replication", newViewId);
      } else {
         log.tracef("Replicating: chOld = %s, chNew = %s", chOld, chNew);

         if (configuration.isStateTransferEnabled() && !initialView) {
            // Contains the state to be pushed to various servers. The state is a hashmap of keys and values
            final Collection<InternalCacheEntry> state = new ArrayList<InternalCacheEntry>();

            for (InternalCacheEntry ice : dataContainer) {
               replicate(ice.getKey(), ice, chOld, chNew, null, state);
            }

            // Only fetch the data from the cache store if the cache store is not shared
            CacheStore cacheStore = stateTransferManager.getCacheStoreForStateTransfer();
            if (cacheStore != null) {
               for (Object key : cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer))) {
                  replicate(key, null, chOld, chNew, cacheStore, state);
               }
            } else {
               if (trace) log.trace("No cache store or cache store is shared, not replicating stored keys");
            }

            // Now for each joiner S : push state to S via RPC
            // TODO We are sending the same stuff to all the joiners, we should use multicast instead
            final Map<Address, Collection<InternalCacheEntry>> states = new HashMap<Address, Collection<InternalCacheEntry>>();
            for (Address joiner : joiners) {
               states.put(joiner, state);
            }
            pushState(states);
         } else {
            if (!initialView) log.trace("State transfer not enabled, so not pushing state");
         }
      }
   }


   /**
    * Computes the list of old and new servers for a given key K and value V. Adds (K, V) to the <code>states</code> map
    * if K should be pushed to other servers. Adds K to the <code>keysToRemove</code> list if this node is no longer an
    * owner for K.
    *
    * @param key          The key
    * @param value        The value; <code>null</code> if the value is not in the data container
    * @param chOld        The old (current) consistent hash
    * @param chNew        The new consistent hash
    * @param cacheStore   If the value is <code>null</code>, try to load it from this cache store
    * @param state       The result collection of entries to be pushed to the joiners
    */
   private void replicate(Object key, InternalCacheEntry value, ConsistentHash chOld, ConsistentHash chNew,
                          CacheStore cacheStore, Collection<InternalCacheEntry> state) {
      // 1. Get the old primary owner for key K
      // That node will be the "pushing owner" for key K
      List<Address> oldOwners = chOld.locate(key, 1);
      Address pushingOwner = oldOwners.size() > 0 ? oldOwners.get(0) : null;

      if (trace) log.tracef("Replicating key %s, pushing owner is %s",
            key, pushingOwner);

      // 2. Push K to all the new nodes
      if (self.equals(pushingOwner)) {
         if (value == null) {
            try {
               value = cacheStore.load(key);
            } catch (CacheLoaderException e) {
               log.failedLoadingValueFromCacheStore(key);
            }
         }

         if (value != null)
            state.add(value);
      }
   }

}
