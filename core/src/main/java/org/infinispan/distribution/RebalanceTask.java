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
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
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

import static org.infinispan.distribution.ch.ConsistentHashHelper.createConsistentHash;

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
   private int newViewId;
   private final CacheNotifier notifier;



   public RebalanceTask(RpcManager rpcManager, CommandsFactory commandsFactory, Configuration conf,
                        DataContainer dataContainer, DistributionManagerImpl dmi,
                        InvocationContextContainer icc, int newViewId,
                        CacheNotifier notifier) {
      super(dmi, rpcManager, conf, commandsFactory, dataContainer);
      this.icc = icc;
      this.newViewId = newViewId;
      this.notifier = notifier;
   }


   protected void performRehash() throws Exception {
      long start = System.currentTimeMillis();
      if (log.isDebugEnabled())
         log.debugf("Commencing rehash on node: %s. Before start, distributionManager.joinComplete = %s", getMyAddress(), distributionManager.isJoinComplete());
      ConsistentHash chOld, chNew;
      try {
         // 1.  Get the old CH
         chOld = distributionManager.getConsistentHash();

         // 2. Create the new CH:
         List<Address> newMembers = rpcManager.getTransport().getMembers();
         chNew = createConsistentHash(configuration, newMembers);
         notifier.notifyTopologyChanged(chOld, chNew, true);
         distributionManager.setConsistentHash(chNew);
         notifier.notifyTopologyChanged(chOld, chNew, false);

         if (log.isTraceEnabled()) {
            log.tracef("Rebalancing\nchOld = %s\nchNew = %s", chOld, chNew);
         }

         if (configuration.isRehashEnabled()) {
            // Cache sets for notification
            Collection<Address> oldCacheSet = Immutables.immutableCollectionWrap(chOld.getCaches());
            Collection<Address> newCacheSet = Immutables.immutableCollectionWrap(chNew.getCaches());

            // notify listeners that a rehash is about to start
            notifier.notifyDataRehashed(oldCacheSet, newCacheSet, newViewId, true);

            List<Object> removedKeys = new ArrayList<Object>();
            NotifyingNotifiableFuture<Object> stateTransferFuture = new AggregatingNotifyingFutureImpl(null, newMembers.size());

            try {
               // Don't need to log anything, all transactions will be blocked
               //distributionManager.getTransactionLogger().enable();
               distributionManager.getTransactionLogger().blockNewTransactions();

               int numOwners = configuration.getNumOwners();

               // Contains the state to be pushed to various servers. The state is a hashmap of keys and values
               final Map<Address, Map<Object, InternalCacheValue>> states = new HashMap<Address, Map<Object, InternalCacheValue>>();

               for (InternalCacheEntry ice : dataContainer) {
                  rebalance(ice.getKey(), ice, numOwners, chOld, chNew, null, states, removedKeys);
               }

               // Only fetch the data from the cache store if the cache store is not shared
               CacheStore cacheStore = distributionManager.getCacheStoreForRehashing();
               if (cacheStore != null) {
                  for (Object key : cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer))) {
                     rebalance(key, null, numOwners, chOld, chNew, cacheStore, states, removedKeys);
                  }
               } else {
                  if (trace) log.trace("Shared cache store or fetching of persistent state disabled");
               }

               // Now for each server S in states.keys(): push states.get(S) to S via RPC
               for (Map.Entry<Address, Map<Object, InternalCacheValue>> entry : states.entrySet()) {
                  final Address target = entry.getKey();
                  Map<Object, InternalCacheValue> state = entry.getValue();
                  if (trace)
                     log.tracef("pushing %d keys to %s", state.size(), target);

                  final RehashControlCommand cmd = cf.buildRehashControlCommand(RehashControlCommand.Type.APPLY_STATE, self,
                                                                                state, chOld, chNew);

                  rpcManager.invokeRemotelyInFuture(Collections.singleton(target), cmd,
                                                    false, stateTransferFuture, configuration.getRehashRpcTimeout());
               }
            } finally {
               distributionManager.getTransactionLogger().unblockNewTransactions();
            }

            // wait to see if all servers received the new state
            // TODO should we retry the state transfer operation if it failed on some of the nodes?
            try {
               stateTransferFuture.get();
            } catch (ExecutionException e) {
               log.error("Error transferring state to node after rehash:", e);
            }

            // Notify listeners of completion of rehashing
            notifier.notifyDataRehashed(oldCacheSet, newCacheSet, newViewId, false);

            // now we can invalidate the keys
            try {
               InvalidateCommand invalidateCmd = cf.buildInvalidateFromL1Command(true, removedKeys);
               InvocationContext ctx = icc.createNonTxInvocationContext();
               invalidateCmd.perform(ctx);
            } catch (Throwable t) {
               log.error("Error invalidating from L1", t);
            }

            if (trace) {
               if (removedKeys.size() > 0)
                  log.tracef("removed %d keys", removedKeys.size());
               log.tracef("data container has now %d keys", dataContainer.size());
            }
         } else {
            if (trace) log.trace("Rehash not enabled, so not pushing state");
         }

         // now we can inform the coordinator that we have finished our push
         Transport t = rpcManager.getTransport();
         if (t.isCoordinator()) {
            distributionManager.markNodePushCompleted(t.getViewId(), t.getAddress());
         } else {
            final RehashControlCommand cmd = cf.buildRehashControlCommand(RehashControlCommand.Type.NODE_PUSH_COMPLETED, self, newViewId);

            // doesn't matter when the coordinator receives the command, the transport will ensure that it eventually gets there
            rpcManager.invokeRemotely(Collections.singleton(t.getCoordinator()), cmd, false);
         }
      } catch (Exception e) {
         log.error("failure in rebalancing", e);
         throw new CacheException("Unexpected exception", e);
      } finally {
         log.debugf("%s completed join rehash in %s!", self, Util.prettyPrintTime(System.currentTimeMillis() - start));
      }
   }


   /**
    * Computes the list of old and new servers for a given key K and value V. Adds (K, V) to the <code>states</code> map
    * if K should be pushed to other servers. Adds K to the <code>removedKeys</code> list if this node is no longer an
    * owner for K.
    *
    * @param key         The key
    * @param value       The value; <code>null</code> if the value is not in the data container
    * @param numOwners   The number of owners (grabbed from the configuration)
    * @param chOld       The old (current) consistent hash
    * @param chNew       The new consistent hash
    * @param cacheStore  If the value is <code>null</code>, try to load it from this cache store
    * @param states      The result hashmap. Keys are servers, values are states (hashmaps) to be pushed to them
    * @param removedKeys A list that the keys that we need to remove will be added to
    */
   protected void rebalance(Object key, InternalCacheEntry value, int numOwners, ConsistentHash chOld, ConsistentHash chNew,
                            CacheStore cacheStore, Map<Address, Map<Object, InternalCacheValue>> states, List<Object> removedKeys) {
      // 1. Get the old and new servers for key K
      List<Address> oldServers = chOld.locate(key, numOwners);
      List<Address> newServers = chNew.locate(key, numOwners);

      // 2. If the target set for K hasn't changed --> no-op
      if (oldServers.equals(newServers))
         return;

      // 3. The old owner is the last node in the old server list that's also in the new server list
      // This might be null if we have old servers={A,B} and new servers={C,D}
      Address oldOwner = null;
      for (int i = oldServers.size() - 1; i >= 0; i--) {
         Address tmp = oldServers.get(i);
         if (newServers.contains(tmp)) {
            oldOwner = tmp;
            break;
         }
      }

      // 4. Push K to all the new servers which are *not* in the old servers list
      if (self.equals(oldOwner)) {
         if (value == null) {
            try {
               value = cacheStore.load(key);
            } catch (CacheLoaderException e) {
               log.warnf("failed loading value for key %s from cache store", key);
            }
         }

         for (Address server : newServers) {
            if (!oldServers.contains(server)) { // server doesn't have K
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

      // TODO do we really need to check if oldServers.contains(self) ?
      // 5. Remove K if it should not be stored here any longer; rebalancing moved K to a different server
      if (oldServers.contains(self) && !newServers.contains(self)) {
         removedKeys.add(key);
      }
   }


   public Address getMyAddress() {
      return rpcManager != null && rpcManager.getTransport() != null ? rpcManager.getTransport().getAddress() : null;
   }


}
