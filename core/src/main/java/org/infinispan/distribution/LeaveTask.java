package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * A task to handle rehashing for when a node leaves the cluster
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class LeaveTask extends RehashTask {
   private static final Log log = LogFactory.getLog(LeaveTask.class);

   private final List<Address> leavers;
   private final Address self;
   private final List<Address> leaversHandled;


   protected LeaveTask(DistributionManagerImpl dmi, RpcManager rpcManager, Configuration configuration, List<Address> leavers,
                       TransactionLogger transactionLogger, CommandsFactory cf, DataContainer dataContainer) {
      super(dmi, rpcManager, configuration, transactionLogger, cf, dataContainer);
      this.leavers = leavers;
      this.leaversHandled = new LinkedList<Address>(leavers);
      this.self = rpcManager.getTransport().getAddress();
   }

   protected void performRehash() throws Exception {
      long start = System.currentTimeMillis();
      if (log.isDebugEnabled()) log.debug("Commencing.  Leavers' list is {0}", leavers);
      boolean completedSuccessfully = false;
      List<Address> leaversHandled = new LinkedList<Address>(leavers);
      ConsistentHash oldCH = ConsistentHashHelper.createConsistentHash(configuration, dmi.getConsistentHash().getCaches(), leaversHandled);
      int replCount = configuration.getNumOwners();
      try {
         StateMap statemap = new StateMap(leaversHandled, self, oldCH, dmi.getConsistentHash(), replCount);
         if (log.isTraceEnabled()) log.trace("Examining state in data container");
         // need to constantly detect whether we are interrupted.  If so, abort accordingly.
         for (InternalCacheEntry ice : dataContainer) {
            List<Address> oldOwners = oldCH.locate(ice.getKey(), replCount);
            for (Address a : oldOwners) if (leaversHandled.contains(a)) statemap.addState(ice);
         }

         CacheStore cs = dmi.getCacheStoreForRehashing();
         if (cs != null) {
            if (log.isTraceEnabled()) log.trace("Examining state in cache store");
            for (InternalCacheEntry ice : cs.loadAll()) if (!statemap.containsKey(ice.getKey())) statemap.addState(ice);
         }

         // push state.
         Set<Future<Void>> pushFutures = new HashSet<Future<Void>>();
         for (Map.Entry<Address, Map<Object, InternalCacheValue>> entry : statemap.getState().entrySet()) {
            if (log.isDebugEnabled()) log.debug("Pushing {0} entries to {1}", entry.getValue().size(), entry.getKey());
            RehashControlCommand push = cf.buildRehashControlCommand(RehashControlCommand.Type.PUSH_STATE, self, entry.getValue());
            NotifyingNotifiableFuture f = new NotifyingFutureImpl(null);
            pushFutures.add(f);
            rpcManager.invokeRemotelyInFuture(Collections.singleton(entry.getKey()), push, true, f, configuration.getRehashRpcTimeout());
         }

         for (Future f : pushFutures) f.get();

         completedSuccessfully = true;
         invalidateInvalidHolders(oldCH, dmi.getConsistentHash());
         if (log.isInfoEnabled())
            log.info("Completed in {0}!", Util.prettyPrintTime(System.currentTimeMillis() - start));
      } catch (InterruptedException ie) {
         if (log.isInfoEnabled())
            log.info("Interrupted after {0}!  Completed successfully? {1}", Util.prettyPrintTime(System.currentTimeMillis() - start), completedSuccessfully);
      } catch (Exception e) {
         log.error("Caught exception! Completed successfully? {0}", e, completedSuccessfully);
      }
      finally {
         if (completedSuccessfully) leavers.removeAll(leaversHandled);
      }
   }

   @Override
   protected Collection<Address> getInvalidHolders(Object key, ConsistentHash chOld, ConsistentHash chNew) {
      Collection<Address> l = super.getInvalidHolders(key, chOld, chNew);
      l.removeAll(leaversHandled);
      return l;
   }
}

class StateMap {
   List<Address> leavers;
   Address self;
   ConsistentHash oldCH, newCH;
   int replCount;
   Set<Object> keysHandled = new HashSet<Object>();
   Map<Address, Map<Object, InternalCacheValue>> state = new HashMap<Address, Map<Object, InternalCacheValue>>();

   StateMap(List<Address> leavers, Address self, ConsistentHash oldCH, ConsistentHash newCH, int replCount) {
      this.leavers = leavers;
      this.self = self;
      this.oldCH = oldCH;
      this.newCH = newCH;
      this.replCount = replCount;
   }

   /**
    * Only add state to state map if old_owner_list for key contains a leaver, and the position of the leaver in the old
    * owner list
    *
    * @param ice
    */
   void addState(InternalCacheEntry ice) {
      for (Address leaver : leavers) {
         List<Address> owners = oldCH.locate(ice.getKey(), replCount);
         int leaverIndex = owners.indexOf(leaver);
         if (leaverIndex > -1) {
            int numOwners = owners.size();
            int selfIndex = owners.indexOf(self);
            boolean isLeaverLast = leaverIndex == numOwners - 1;
            if ((isLeaverLast && selfIndex == numOwners - 2) ||
                  (!isLeaverLast && selfIndex == leaverIndex + 1)) {
               // add to state map!
               List<Address> newOwners = newCH.locate(ice.getKey(), replCount);
               newOwners.removeAll(owners);
               if (!newOwners.isEmpty()) {
                  for (Address no : newOwners) {
                     Map<Object, InternalCacheValue> s = state.get(no);
                     if (s == null) {
                        s = new HashMap<Object, InternalCacheValue>();
                        state.put(no, s);
                     }
                     s.put(ice.getKey(), ice.toInternalCacheValue());
                  }
               }
            }
         }
      }
      keysHandled.add(ice.getKey());
   }

   Map<Address, Map<Object, InternalCacheValue>> getState() {
      return state;
   }

   boolean containsKey(Object key) {
      return keysHandled.contains(key);
   }
}
