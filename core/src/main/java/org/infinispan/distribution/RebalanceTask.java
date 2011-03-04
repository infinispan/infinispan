package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.Util;

import java.util.*;
import java.util.concurrent.Callable;

import static org.infinispan.commands.control.RehashControlCommand.Type.APPLY_STATE;
import static org.infinispan.distribution.ch.ConsistentHashHelper.createConsistentHash;
import static org.infinispan.remoting.rpc.ResponseMode.SYNCHRONOUS;

/**
 * Task which handles view changes (joins, merges or leaves) and rebalances keys using a push based approach. Essentially,
 * every member gets the old and new consistent hash (CH) on a view change. Then for each key K, it gets the target
 * servers S-old for the old CH and S-new for the new CH. If S-old == S-new, it does nothing. If there is a change, it
 * either pushes K to the new location (if it is the owner), or invalidates K (if we're not the owner any longer).<p/>
 * Example:<p/>
 * <pre>
 * - The membership is {A,B,C,D}
 * - The new view is {A,B,C,D,E,F}
 * - For K, the old CH is A,B and the new CH is A,C
 * - A (since it is K's owner) now pushes K to C
 * - B invalidates K
 * - For K2, the old CH is A,B and the new CH is B,C
 * - B (since it is the backup owner and A left) pushes K2 to C
 * </pre>
 * @author Bela Ban
 * @since 4.2
 */
public class RebalanceTask extends RehashTask {
   private final InboundInvocationHandler inboundInvocationHandler;


   public RebalanceTask(RpcManager rpcManager, CommandsFactory commandsFactory, Configuration conf,
                        DataContainer dataContainer, DistributionManagerImpl dmi, InboundInvocationHandler inboundInvocationHandler) {
      super(dmi, rpcManager, conf, commandsFactory, dataContainer);
      this.inboundInvocationHandler = inboundInvocationHandler;
   }



   protected void performRehash() throws Exception {
      long start = System.currentTimeMillis();
      ConsistentHash chOld, chNew;
      try {
         // 1.  Get the old CH
         chOld=distributionManager.getConsistentHash();

         // 2. Create the new CH:
         List<Address> newMembers=new ArrayList<Address>(rpcManager.getTransport().getMembers());
         newMembers.remove(self);
         chNew=createConsistentHash(configuration, newMembers, distributionManager.getTopologyInfo(), self);
         distributionManager.setConsistentHash(chNew);

         try {
            if (configuration.isRehashEnabled()) {
               distributionManager.getTransactionLogger().enable(); // todo: check
               distributionManager.getTransactionLogger().blockNewTransactions();

               int numOwners = configuration.getNumOwners();

               // Contains the state to be pushed to various servers. The state is a hashmap of keys and values
               final Map<Address,Map<Object,InternalCacheValue>> states=new HashMap<Address,Map<Object,InternalCacheValue>>();

               int removed=0;
               for (InternalCacheEntry ice : dataContainer) {
                  removed+=rebalance(ice.getKey(), numOwners, chOld, chNew, states, dataContainer, null);
               }

               CacheStore cacheStore = distributionManager.getCacheStoreForRehashing();
               if (cacheStore != null) {
                  for (Object k: cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer))) {
                     removed+=rebalance(k, numOwners, chOld, chNew, states, null, cacheStore);
                  }
               }

               // Now for each server S in states.keys(): push states.get(S) to S via RPC
               for(Map.Entry<Address,Map<Object,InternalCacheValue>> entry: states.entrySet()) {
                  final Address target=entry.getKey();
                  Map<Object,InternalCacheValue> state = entry.getValue();
                  if(trace)
                     log.trace("pushing %d keys to %s", state.size(), target);

                  final RehashControlCommand cmd = cf.buildRehashControlCommand(APPLY_STATE, self, state, chOld, chNew, null);
                  statePullExecutor.submit(new Callable<Void>() {
                     public Void call() throws Exception {
                        rpcManager.invokeRemotely(Collections.singleton(target), cmd,
                                                  SYNCHRONOUS, configuration.getRehashRpcTimeout(), true);
                        return null;
                     }
                  });
               }

               if(trace) {
                  if(removed > 0)
                     log.trace("removed %d keys", removed);
                  log.trace("data container has now %d keys", dataContainer.size());
               }

               distributionManager.getTransactionLogger().unblockNewTransactions();
            } else {
               if (trace) log.trace("Rehash not enabled, so not pushing state");
            }                                 
         } finally {
            // wait for any enqueued remote commands to finish...
            distributionManager.setJoinComplete(true);
            distributionManager.setRehashInProgress(false);
            inboundInvocationHandler.blockTillNoLongerRetrying(cf.getCacheName());
         }

      } catch (Exception e) {
         log.error("failure in rebalancing", e);
         throw new CacheException("Unexpected exception", e);
      } finally {
         log.info("%s completed rebalancing in %s", self, Util.prettyPrintTime(System.currentTimeMillis() - start));
      }
   }


   /**
    * Computes the list of old a new servers for a given key K and value V. Removes K (from memory or the cache store)
    * if the current server shouldn't store it. Adds K to the 'states' map if K should be pushed to other servers.
    * @param key The key
    * @param numOwners The number of owners (grabbed from the configuration)
    * @param chOld The old (current) consistent hash
    * @param chNew The new consistent hash
    * @param states The result hashmap. Keys are servers, values are states (hashmaps) to be pushed to them.
    * @param container The in-memory cache to be used for removal. Null if we shouldn't remove from the cache
    * @param cacheStore The cache store for removal. Null if we shouldn't remove from the cache store.
    * @return Number of removed keys
    */
   protected int rebalance(Object key, int numOwners, ConsistentHash chOld, ConsistentHash chNew,
                           Map<Address,Map<Object,InternalCacheValue>> states, DataContainer container, CacheStore cacheStore) {
      int removed=0;

      // 1. Get the old and new servers for key K
      List<Address> oldServers = chOld.locate(key, numOwners);
      List<Address> newServers = chNew.locate(key, numOwners);

      // 2. If the target set for K hasn't changed --> no-op
      if(oldServers.equals(newServers))
         return removed;

      // 3. The old owner is the first node in the old server list that's also in the new server list
      // This might be null if we have old servers={A,B} and new servers={C,D}
      Address oldOwner=null;
      for(Address tmp: oldServers) {
         if(newServers.contains(tmp)) {
            oldOwner=tmp;
            break;
         }
      }

      // 4. Push K to all the new servers which are *not* in the old servers list
      if(self.equals(oldOwner)) {
         for(Address server: newServers) {
            if(!oldServers.contains(server)) { // server doesn't have K
               Map<Object,InternalCacheValue> map = states.get(server);
               if(map == null) {
                  map=new HashMap<Object,InternalCacheValue>();
                  states.put(server, map);
               }
               InternalCacheEntry value=null;
               if(container != null)
                  value=container.get(key);
               else if(cacheStore != null) {
                  try {
                     value=cacheStore.load(key);
                  } catch (CacheLoaderException e) {
                     log.warn("failed loading value for key %s from cache store", key);
                  }
               }
               if(value != null)
                  map.put(key, value.toInternalCacheValue());
            }
         }
      }

      // 5. Remove K if it should not be stored here any longer; rebalancing moved K to a different server
      if(oldServers.contains(self) && !newServers.contains(self)) {
         // todo: invalidate (rather than remove) K on self (use InvalidateL1Command)
         if(container != null && container.remove(key) != null)
            removed++;

         if(cacheStore != null) {
            try {
               if(cacheStore.remove(key))
                  removed++;
            } catch (CacheLoaderException e) {
               log.warn("failed removing " + key + " from cache store", e);
            }
         }
      }
      return removed;
   }
   

   public Address getMyAddress() {
      return rpcManager != null && rpcManager.getTransport() != null ? rpcManager.getTransport().getAddress() : null;
   }


}
