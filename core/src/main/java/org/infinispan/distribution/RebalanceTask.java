package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.NotifyingFutureImpl;

import java.util.*;
import java.util.concurrent.Callable;

import static org.infinispan.commands.control.RehashControlCommand.Type.*;
import static org.infinispan.distribution.ch.ConsistentHashHelper.createConsistentHash;
import static org.infinispan.remoting.rpc.ResponseMode.SYNCHRONOUS;

/**
 * Task which handles view changes (joins, merges or leaves) and rebalances keys using a push based approach. Essentially,
 * every member gets the old and new consistent hash (CH) on a view changes. Then for each key K, it gets the target
 * servers S-old for the old CH and S-new for the new CH. If S-old == S-new, it does nothing. If there is a change, it
 * either pushes K to the new location (if it is the owner), or invalidates K (if we're not the owner any longer).<p/>
 * Example:<p/>
 * <pre>
 * - The membership is {A,B,C,D}
 * - The new view is {A,B,C,D,E,F}
 * - For K, the old CH is A,B and the new CH is A,C
 * - A (since it is K's owner) now pushes K to C
 * - B invalidates K
 * - For K2, the old CH is A,B and the new CH is E,F
 * - A (since it is the owner) pushes K2 to E and F
 * - B invalidates K2
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
      if (log.isDebugEnabled())
         log.debug("Commencing rehash on node: %s. Before start, distributionManager.joinComplete = %s", getMyAddress(), distributionManager.isJoinComplete());
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
               Map<Address,Map<Object,InternalCacheValue>> states=new HashMap<Address,Map<Object,InternalCacheValue>>();

               int removed=0;
               for (InternalCacheEntry ice : dataContainer) {
                  Object k = ice.getKey();
                  List<Address> oldServers = chOld.locate(k, numOwners);
                  List<Address> newServers = chNew.locate(k, numOwners);

                  // the target set for K hasn't changed --> no-op
                  if(oldServers.equals(newServers))
                     continue;

                  Address oldOwner=null;
                  for(Address tmp: oldServers) {
                     if(newServers.contains(tmp)) {
                        oldOwner=tmp;
                        break;
                     }
                  }
                  // System.out.println("#" + count++ + ": oldOwner(" + k + "): " + oldOwner + ", old servers: " + oldServers +
                     //                      ", new servers: " + newServers + ", self == owner: " + self.equals(oldOwner));

                  if(self.equals(oldOwner)) {
                     for(Address server: newServers) {
                        if(!oldServers.contains(server)) { // server doesn't have K
                           // state.put(k, ice.toInternalCacheValue());
                           Map<Object,InternalCacheValue> map = states.get(server);
                           if(map == null) {
                              map=new HashMap<Object,InternalCacheValue>();
                              states.put(server, map);
                           }
                           map.put(k, ice.toInternalCacheValue());
                        }
                     }
                  }

                  if(oldServers.contains(self) && !newServers.contains(self)) {
                     // todo: invalidate (rather than remove) K on self
                     InternalCacheEntry tmp_entry = dataContainer.remove(k);
                     if(tmp_entry != null)
                        removed++;
                  }
               }


               // todo: uncomment
               /*CacheStore cacheStore = distributionManager.getCacheStoreForRehashing();
               if (cacheStore != null) {
                  for (Object k: cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer))) {
                     List<Address> oldServers = chOld.locate(k, numOwners);
                     List<Address> newServers = chNew.locate(k, numOwners);

                     // the target set for K hasn't changed --> no-op
                     if(oldServers.equals(newServers))
                        continue;

                     for(Address oldOwner: oldServers) {
                        if(self.equals(oldOwner)) {
                           for(Address server: newServers) {
                              if(!oldServers.contains(server)) { // server doesn't have K
                                 // state.put(k, ice.toInternalCacheValue());
                                 Map<Object,InternalCacheValue> map = states.get(server);
                                 if(map == null) {
                                    map=new HashMap<Object,InternalCacheValue>();
                                    states.put(server, map);
                                 }
                                 map.put(k, cacheStore.load(k).toInternalCacheValue());
                              }
                           }
                           break;
                        }
                     }
                  }
               }*/

               // Now for each server S in states.keys(): push states.get(S) to S via RPC

               // Maybe it is better to send each push as it occurs, because we might accumulate too much state in the
               // hashmap before we finally do the push for each server...

               for(Map.Entry<Address,Map<Object,InternalCacheValue>> entry: states.entrySet()) {
                  final Address target=entry.getKey();
                  Map<Object,InternalCacheValue> state = entry.getValue();

                  System.out.println("==> pushing " + state.size() + " keys to " + target);

                  final RehashControlCommand cmd = cf.buildRehashControlCommand(APPLY_STATE, self, state, chOld, chNew, null);
                  statePullExecutor.submit(new Callable<Void>() {
                     public Void call() throws Exception {
                        rpcManager.invokeRemotely(Collections.singleton(target), cmd,
                                                  SYNCHRONOUS, configuration.getRehashRpcTimeout(), true);
                        return null;
                     }
                  });
               }

               System.out.println("(removed " + removed + " keys)");
               System.out.println("size of the data container is " + dataContainer.size() + "\n");

               distributionManager.getTransactionLogger().unblockNewTransactions();
            } else {
               // broadcastNewCh();
               if (trace) log.trace("Rehash not enabled, so not pulling state.");
            }                                 
         } finally {
            // wait for any enqueued remote commands to finish...
            distributionManager.setJoinComplete(true);
            distributionManager.setRehashInProgress(false);
            inboundInvocationHandler.blockTillNoLongerRetrying(cf.getCacheName());
            rpcManager.broadcastRpcCommandInFuture(cf.buildRehashControlCommand(JOIN_REHASH_END, self), true, new NotifyingFutureImpl(null));
            if (configuration.isRehashEnabled()) invalidateInvalidHolders(chOld, chNew);
         }

      } catch (Exception e) {
         log.error("Caught exception!", e);
         throw new CacheException("Unexpected exception", e);
      } finally {
         log.info("%s completed join rehash in %s!", self, Util.prettyPrintTime(System.currentTimeMillis() - start));
      }
   }



   public Address getMyAddress() {
      return rpcManager != null && rpcManager.getTransport() != null ? rpcManager.getTransport().getAddress() : null;
   }


}
