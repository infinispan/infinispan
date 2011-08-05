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

package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
 * @since 4.2
 */
public class RebalanceTask extends RehashTask {
   private final InvocationContextContainer icc;
   private final CacheNotifier notifier;
   private final InterceptorChain interceptorChain;
   private final int newViewId;
   private final boolean previousRehashWasInterrupted;

   public RebalanceTask(RpcManager rpcManager, CommandsFactory commandsFactory, Configuration conf,
                        DataContainer dataContainer, DistributionManagerImpl dmi,
                        InvocationContextContainer icc, CacheNotifier notifier,
                        InterceptorChain interceptorChain, int newViewId, boolean rehashInterrupted) {
      super(dmi, rpcManager, conf, commandsFactory, dataContainer);
      this.icc = icc;
      this.notifier = notifier;
      this.interceptorChain = interceptorChain;
      this.newViewId = newViewId;
      this.previousRehashWasInterrupted = rehashInterrupted;
   }


   protected void performRehash() throws Exception {
      long start = System.currentTimeMillis();
      if (log.isDebugEnabled())
         log.debugf("Commencing rehash %d on node: %s. Before start, data container had %d entries",
               newViewId, getMyAddress(), dataContainer.size());
      Collection<Address> oldCacheSet = Collections.emptySet(), newCacheSet = Collections.emptySet();
      List<Object> keysToRemove = new ArrayList<Object>();
      boolean anotherRehashIsPending = false;

      try {
         // Don't need to log anything, all transactions will be blocked
         //distributionManager.getTransactionLogger().enable();
         if (previousRehashWasInterrupted) {
            log.tracef("Rehash is still in progress, not blocking transactions as they should already be blocked");
         } else {
            // if the previous rehash was interrupted by the arrival of a new view
            // then the transactions are still blocked, we don't need to block them again
            distributionManager.getTransactionLogger().blockNewTransactions();
         }

         // Create the new CH:
         List<Address> newMembers = rpcManager.getTransport().getMembers();
         ConsistentHash chNew = ConsistentHashHelper.createConsistentHash(configuration, newMembers);
         ConsistentHash chOld = distributionManager.setConsistentHash(chNew);

         if (trace) {
            log.tracef("Rebalancing: chOld = %s, chNew = %s", chOld, chNew);
         }

         if (configuration.isRehashEnabled()) {
            // Cache sets for notification
            oldCacheSet = Immutables.immutableCollectionWrap(chOld.getCaches());
            newCacheSet = Immutables.immutableCollectionWrap(chNew.getCaches());

            // notify listeners that a rehash is about to start
            notifier.notifyDataRehashed(oldCacheSet, newCacheSet, newViewId, true);

            int numOwners = configuration.getNumOwners();

            // Contains the state to be pushed to various servers. The state is a hashmap of keys and values
            final Map<Address, Map<Object, InternalCacheValue>> states = new HashMap<Address, Map<Object, InternalCacheValue>>();

            for (InternalCacheEntry ice : dataContainer) {
               rebalance(ice.getKey(), ice, numOwners, chOld, chNew, null, states, keysToRemove);
            }

            // Only fetch the data from the cache store if the cache store is not shared
            CacheStore cacheStore = distributionManager.getCacheStoreForRehashing();
            if (cacheStore != null) {
               for (Object key : cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer))) {
                  rebalance(key, null, numOwners, chOld, chNew, cacheStore, states, keysToRemove);
               }
            } else {
               if (trace) log.trace("No cache store or cache store is shared, not rebalancing stored keys");
            }

            // Now for each server S in states.keys(): push states.get(S) to S via RPC
            pushState(chOld, chNew, states);
         } else {
            if (trace) log.trace("Rehash not enabled, so not pushing state");
         }
      } finally {
         try {
            // now we can inform the coordinator that we have finished our push
            distributionManager.notifyCoordinatorPushCompleted(newViewId);

            // wait to unblock transactions until every node has confirmed pushing state
            // if there is another pending rehash the call will return early and
            // DistributionManager.isRehashInProgress() will return true
            anotherRehashIsPending = !distributionManager.waitForRehashToComplete(newViewId);

            if (!anotherRehashIsPending && configuration.isRehashEnabled()) {
               // now we can invalidate the keys
               invalidateKeys(keysToRemove);

               notifier.notifyDataRehashed(oldCacheSet, newCacheSet, newViewId, false);
            }
         } finally {
            if (anotherRehashIsPending) {
               log.debugf("Another rehash is pending, keeping the transactions blocked");
            } else {
               try {
                  distributionManager.getTransactionLogger().unblockNewTransactions();
               } catch (Exception e) {
                  log.debug("Unblocking transactions failed", e);
               }
               distributionManager.markRehashTaskCompleted();
            }
         }
         log.debugf("Node %s completed rehash for view %d in %s!", self, newViewId,
               Util.prettyPrintTime(System.currentTimeMillis() - start));
      }
   }

   private void invalidateKeys(List<Object> keysToRemove) {
      try {
         if (keysToRemove.size() > 0) {
            InvalidateCommand invalidateCmd = cf.buildInvalidateFromL1Command(true, keysToRemove);
            InvocationContext ctx = icc.createNonTxInvocationContext();
            ctx.setFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_LOCKING);
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Invalidated %d keys, data container now has %d keys", keysToRemove.size(), dataContainer.size());
            log.tracef("Invalidated keys: %s", keysToRemove);
         }
      } catch (CacheException e) {
         log.failedToInvalidateKeys(e);
         throw e;
      }
   }

   private void pushState(ConsistentHash chOld, ConsistentHash chNew, Map<Address, Map<Object, InternalCacheValue>> states) throws InterruptedException, ExecutionException {
      NotifyingNotifiableFuture<Object> stateTransferFuture = new AggregatingNotifyingFutureImpl(null, states.size());
      for (Map.Entry<Address, Map<Object, InternalCacheValue>> entry : states.entrySet()) {
         final Address target = entry.getKey();
         Map<Object, InternalCacheValue> state = entry.getValue();
         log.debugf("Pushing to node %s %d keys", target, state.size());
         log.tracef("Pushing to node %s keys: %s", target, state.keySet());

         final RehashControlCommand cmd = cf.buildRehashControlCommand(RehashControlCommand.Type.APPLY_STATE, self,
                                                                       newViewId, state, chOld, chNew);

         rpcManager.invokeRemotelyInFuture(Collections.singleton(target), cmd,
                                           false, stateTransferFuture, configuration.getRehashRpcTimeout());
      }

      // wait to see if all servers received the new state
      // TODO we might want to retry the state transfer operation if it failed on some of the nodes and the view hasn't changed
      try {
         stateTransferFuture.get();
      } catch (ExecutionException e) {
         log.errorTransferringState(e);
         throw e;
      }
      log.debugf("Node finished pushing data for rehash %d.", newViewId);
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
   protected void rebalance(Object key, InternalCacheEntry value, int numOwners, ConsistentHash chOld, ConsistentHash chNew,
                            CacheStore cacheStore, Map<Address, Map<Object, InternalCacheValue>> states, List<Object> keysToRemove) {
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
               Map<Object, InternalCacheValue> map = states.get(server);
               if (map == null) {
                  map = new HashMap<Object, InternalCacheValue>();
                  states.put(server, map);
               }
               if (value != null)
                  map.put(key, value.toInternalCacheValue());
            }
         }
      }

      // 5. Remove K if it should not be stored here any longer; rebalancing moved K to a different server
      if (oldOwners.contains(self) && !newOwners.contains(self)) {
         keysToRemove.add(key);
      }
   }


   public Address getMyAddress() {
      return rpcManager != null && rpcManager.getTransport() != null ? rpcManager.getTransport().getAddress() : null;
   }


}
