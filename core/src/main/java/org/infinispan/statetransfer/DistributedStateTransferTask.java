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
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.Immutables;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Task which handles view changes (joins, merges or leaves) and rebalances keys using a push based approach.
 * Essentially, every member gets the old and new consistent hash (CH) on a view change. Then for each key K, it gets
 * the target servers S-old for the old CH and S-new for the new CH. If S-old == S-new, it does nothing. If there is a
 * change, it either pushes K to the new location (if it is the owner), or invalidates K (if we're not the owner any
 * longer).<p/> Example:<p/>
 * <pre>
 * - The membership is {A,B,C,D}
 * - The new view is {A,B,C,D,E,F}
 * - For K, the old CH is A,B and the new CH is A,C
 * - A (since it is K's owner) now pushes K to C
 * - B invalidates K
 * - For K2, the old CH is A,B and the new CH is B,C
 * - B (since it is the backup owner and A left) pushes K2 to C
 * </pre>
 *
 * @author Bela Ban
 * @author Dan Berindei <dan@infinispan.org>
 * @author Mircea Markus
 * @since 4.2
 */
public class DistributedStateTransferTask extends BaseStateTransferTask {
   private static final Log log = LogFactory.getLog(DistributedStateTransferTask.class);

   private final DistributionManager dm;
   private final DistributedStateTransferManagerImpl stateTransferManager;
   private List<Object> keysToRemove;
   private Collection<Address> oldCacheSet;
   private Collection<Address> newCacheSet;
   private TransactionTable transactionTable;

   public DistributedStateTransferTask(RpcManager rpcManager, Configuration configuration, DataContainer dataContainer,
                                       DistributedStateTransferManagerImpl stateTransferManager,
                                       DistributionManager dm, StateTransferLock stateTransferLock,
                                       CacheNotifier cacheNotifier, int newViewId, Collection<Address> members,
                                       ConsistentHash chOld, ConsistentHash chNew, boolean initialView, TransactionTable transactionTable) {
      super(stateTransferManager, rpcManager, stateTransferLock, cacheNotifier, configuration, dataContainer, members, newViewId, chNew, chOld, initialView);
      this.dm = dm;
      this.stateTransferManager = stateTransferManager;

      // Cache sets for notification
      oldCacheSet = chOld != null ? Immutables.immutableCollectionWrap(chOld.getCaches()) : Collections.<Address>emptySet();
      newCacheSet = Immutables.immutableCollectionWrap(chNew.getCaches());
      this.transactionTable = transactionTable;
   }


   @Override
   public void doPerformStateTransfer() throws Exception {
      if (!stateTransferManager.startStateTransfer(newViewId, members, initialView))
         return;

      if (log.isDebugEnabled())
         log.debugf("Commencing rehash %d on node: %s. Before start, data container had %d entries",
               newViewId, self, dataContainer.size());
      newCacheSet = Collections.emptySet();
      oldCacheSet = Collections.emptySet();
      keysToRemove = new ArrayList<Object>();

      // Don't need to log anything, all transactions will be blocked
      //distributionManager.getTransactionLogger().enable();
      stateTransferLock.blockNewTransactions(newViewId);

      if (trace) {
         log.tracef("Rebalancing: chOld = %s, chNew = %s", chOld, chNew);
      }

      if (configuration.isRehashEnabled() && !initialView) {

         // notify listeners that a rehash is about to start
         cacheNotifier.notifyDataRehashed(oldCacheSet, newCacheSet, newViewId, true);

         int numOwners = configuration.getNumOwners();

         // Contains the state to be pushed to various servers. The state is a hashmap of servers to entry collections
         final Map<Address, Collection<InternalCacheEntry>> states = new HashMap<Address, Collection<InternalCacheEntry>>();

         for (InternalCacheEntry ice : dataContainer) {
            rebalance(ice.getKey(), ice, numOwners, chOld, chNew, null, states, keysToRemove);
         }

         checkIfCancelled();

         // Only fetch the data from the cache store if the cache store is not shared
         CacheStore cacheStore = stateTransferManager.getCacheStoreForStateTransfer();
         if (cacheStore != null) {
            for (Object key : cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer))) {
               rebalance(key, null, numOwners, chOld, chNew, cacheStore, states, keysToRemove);
            }
         } else {
            if (trace) log.trace("No cache store or cache store is shared, not rebalancing stored keys");
         }

         checkIfCancelled();

         // Push any remaining state chunks
         for (Map.Entry<Address, Collection<InternalCacheEntry>> entry : states.entrySet()) {
            pushPartialState(Collections.singleton(entry.getKey()), entry.getValue(), null);
         }
         
         // Push locks if the cache is transactional and it is distributed
         if (transactionTable != null) {
            log.debug("Starting lock migration");
            Map<Address, Collection<LockInfo>> locksToMigrate = new HashMap<Address, Collection<LockInfo>>();
            rebalanceLocks(numOwners, locksToMigrate, transactionTable.getRemoteTransactions());
            rebalanceLocks(numOwners, locksToMigrate, transactionTable.getLocalTransactions());
            for (Map.Entry<Address, Collection<LockInfo>> e : locksToMigrate.entrySet()) {
               pushPartialState(Collections.singleton(e.getKey()), null, e.getValue());
            }
         }
         
         // And wait for all the push RPCs to end
         finishPushingState();
      } else {
         if (!initialView) log.trace("Rehash not enabled, so not pushing state");
      }
   }

   private void rebalanceLocks(int numOwners, Map<Address, Collection<LockInfo>> locksToMigrate, Collection<? extends CacheTransaction> tx) throws StateTransferCancelledException {
      for (CacheTransaction cacheTx : tx) {
         for (Object key : cacheTx.getLockedKeys()) {
            Address oldLockOwner = self;
            Address newLockOwner = chNew.locate(key, numOwners).get(0);
            if (!oldLockOwner.equals(newLockOwner)) {
               log.tracef("Migrating lock %s from node %s to ", key, oldLockOwner, newLockOwner);
               Collection<LockInfo> lockInfo = locksToMigrate.get(newLockOwner);
               if (lockInfo == null) {
                  lockInfo = new ArrayList<LockInfo>();
                  locksToMigrate.put(newLockOwner, lockInfo);
               }
               lockInfo.add(new LockInfo(cacheTx.getGlobalTransaction(),key));
               if (lockInfo.size() > stateTransferChunkSize) {
                  pushPartialState(Collections.singleton(newLockOwner), null, lockInfo);
                  locksToMigrate.remove(newLockOwner);
               }
            }
         }
      }
   }

   public void commitStateTransfer() {
      // update the distribution manager's consistent hash
      dm.setConsistentHash(chNew);

      if (configuration.isRehashEnabled() && !initialView) {
         // now we can invalidate the keys
         stateTransferManager.invalidateKeys(keysToRemove);

         cacheNotifier.notifyDataRehashed(oldCacheSet, newCacheSet, newViewId, false);
      }

      super.commitStateTransfer();
   }


   /**
    * Computes the list of old and new servers for a given key K and value V. Adds (K, V) to the <code>states</code> map
    * if K should be pushed to other servers. Adds K to the <code>keysToRemove</code> list if this node is no longer an
    * owner for K.
    *
    * @param key          The key
    * @param value        The value; <code>null</code> if the value is not in the data container
    * @param numOwners    The number of owners (grabbed from the configuration)
    * @param chOld        The old (current) consistent hash
    * @param chNew        The new consistent hash
    * @param cacheStore   If the value is <code>null</code>, try to load it from this cache store
    * @param states       The result hashmap. Keys are servers, values are states (hashmaps) to be pushed to them
    * @param keysToRemove A list that the keys that we need to remove will be added to
    */
   private void rebalance(Object key, InternalCacheEntry value, int numOwners, ConsistentHash chOld, ConsistentHash chNew,
                            CacheStore cacheStore, Map<Address, Collection<InternalCacheEntry>> states, List<Object> keysToRemove) throws StateTransferCancelledException {
      // 1. Get the old and new servers for key K
      List<Address> oldOwners = chOld.locate(key, numOwners);
      List<Address> newOwners = chNew.locate(key, numOwners);

      // 2. If the target set for K hasn't changed --> no-op
      if (oldOwners.equals(newOwners))
         return;

      // 3. The pushing server is the last node in the old owner list that's also in the new CH
      // It will only be null if all the old owners left the cluster
      Address pushingOwner = null;
      for (int i = oldOwners.size() - 1; i >= 0; i--) {
         Address server = oldOwners.get(i);
         if (chNew.getCaches().contains(server)) {
            pushingOwner = server;
            break;
         }
      }

      if (trace) log.tracef("Rebalancing key %s from %s to %s, pushing owner is %s",
            key, oldOwners, newOwners, pushingOwner);

      // 4. Push K to all the new servers which are *not* in the old servers list
      if (self.equals(pushingOwner)) {
         if (value == null) {
            try {
               value = cacheStore.load(key);
            } catch (CacheLoaderException e) {
               log.failedLoadingValueFromCacheStore(key);
            }
         }

         for (Address server : newOwners) {
            if (!oldOwners.contains(server)) { // server doesn't have K
               Collection<InternalCacheEntry> stateForANode = states.get(server);
               if (stateForANode == null) {
                  stateForANode = new ArrayList<InternalCacheEntry>();
                  states.put(server, stateForANode);
               }
               if (value != null)
                  stateForANode.add(value);

               // if we have a full chunk, start pushing it to the new owner
               if (stateForANode.size() >= stateTransferChunkSize) {
                  pushPartialState(Collections.singleton(server), stateForANode, null);
                  states.remove(server);
               }
            }
         }
      }

      // 5. Remove K if it should not be stored here any longer; rebalancing moved K to a different server
      if (!newOwners.contains(self)) {
         keysToRemove.add(key);
      }
   }

}
