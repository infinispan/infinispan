package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A task to handle rehashing for when a node leaves the cluster
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class LeaveTask extends RehashTask {
   private static final Log log = LogFactory.getLog(LeaveTask.class);
   private static final boolean trace = log.isTraceEnabled();
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
         InMemoryStateMap statemap = new InMemoryStateMap(leaversHandled, self, oldCH, dmi.getConsistentHash(), replCount);
         if (log.isTraceEnabled()) log.trace("Examining state in data container");
         // need to constantly detect whether we are interrupted.  If so, abort accordingly.
         for (InternalCacheEntry ice : dataContainer) {
            List<Address> oldOwners = oldCH.locate(ice.getKey(), replCount);
            for (Address a : oldOwners) if (leaversHandled.contains(a)) statemap.addState(ice);
         }

         CacheStore cs = dmi.getCacheStoreForRehashing();
         if (cs != null) {
            if (log.isTraceEnabled()) log.trace("Examining state in cache store");
            for (InternalCacheEntry ice : cs.loadAll()) if (statemap.doesNotContainKey(ice.getKey())) statemap.addState(ice);
         }

         // push state.
         Set<Future<Object>> pushFutures = new HashSet<Future<Object>>();
         for (Map.Entry<Address, Map<Object, InternalCacheValue>> entry : statemap.getState().entrySet()) {
            if (log.isDebugEnabled()) log.debug("Pushing {0} entries to {1}", entry.getValue().size(), entry.getKey());
            RehashControlCommand push = cf.buildRehashControlCommand(self, entry.getValue());
            NotifyingNotifiableFuture<Object> f = new NotifyingFutureImpl(null);
            pushFutures.add(f);
            rpcManager.invokeRemotelyInFuture(Collections.singleton(entry.getKey()), push, true, f, configuration.getRehashRpcTimeout());
         }

         for (Future f : pushFutures) f.get();

         processAndDrainTxLog(oldCH, dmi.getConsistentHash(), replCount);

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
   protected Log getLog() {
      return log;
   }

   private void processAndDrainTxLog(ConsistentHash oldCH, ConsistentHash newCH, int replCount) {
      if (trace) log.trace("Processing transaction log iteratively");

      List<WriteCommand> c;
      int i = 0;
      while (transactionLogger.shouldDrainWithoutLock()) {
         if (trace) log.trace("Processing transaction log, iteration {0}", i++);
         c = transactionLogger.drain();
         if (trace) log.trace("Found {0} modifications", c.size());
         apply(oldCH, newCH, replCount, c);
      }

      if (trace) log.trace("Processing transaction log: final drain and lock");
      c = transactionLogger.drainAndLock();
      if (trace) log.trace("Found {0} modifications", c.size());
      apply(oldCH, newCH, replCount, c);

      if (trace) log.trace("Handling pending prepares");
      PendingPreparesMap state = new PendingPreparesMap(leavers, oldCH, newCH, replCount);
      Collection<PrepareCommand> pendingPrepares = transactionLogger.getPendingPrepares();
      if (trace) log.trace("Found {0} pending prepares", pendingPrepares.size());
      for (PrepareCommand pc : pendingPrepares) state.addState(pc);

      if (trace) log.trace("State map for pending prepares is {0}", state.getState());

      Set<Future<Object>> pushFutures = new HashSet<Future<Object>>();
      for (Map.Entry<Address, List<PrepareCommand>> e : state.getState().entrySet()) {
         if (log.isDebugEnabled())
            log.debug("Pushing {0} uncommitted prepares to {1}", e.getValue().size(), e.getKey());
         RehashControlCommand push = cf.buildRehashControlCommandTxLogPendingPrepares(self, e.getValue());
         NotifyingNotifiableFuture<Object> f = new NotifyingFutureImpl(null);
         pushFutures.add(f);
         rpcManager.invokeRemotelyInFuture(Collections.singleton(e.getKey()), push, true, f, configuration.getRehashRpcTimeout());
      }

      for (Future f : pushFutures) {
         try {
            f.get();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         } catch (ExecutionException e) {
            log.error("Error pushing tx log", e);
         }
      }
      if (trace) log.trace("Finished pushing pending prepares; unlocking and disabling transaction logging");

      transactionLogger.unlockAndDisable();
   }

   private void apply(ConsistentHash oldCH, ConsistentHash newCH, int replCount, List<WriteCommand> wc) {
      // need to create another "state map"
      TransactionLogMap state = new TransactionLogMap(leavers, oldCH, newCH, replCount);
      for (WriteCommand c : wc) state.addState(c);

      if (trace) log.trace("State map for modifications is {0}", state.getState());

      Set<Future<Object>> pushFutures = new HashSet<Future<Object>>();
      for (Map.Entry<Address, List<WriteCommand>> entry : state.getState().entrySet()) {
         if (log.isDebugEnabled())
            log.debug("Pushing {0} modifications to {1}", entry.getValue().size(), entry.getKey());
         RehashControlCommand push = cf.buildRehashControlCommandTxLog(self, entry.getValue());
         NotifyingNotifiableFuture<Object> f = new NotifyingFutureImpl(null);
         pushFutures.add(f);
         rpcManager.invokeRemotelyInFuture(Collections.singleton(entry.getKey()), push, true, f, configuration.getRehashRpcTimeout());
      }

      for (Future f : pushFutures) {
         try {
            f.get();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         } catch (ExecutionException e) {
            log.error("Error pushing tx log", e);
         }
      }
   }

   @Override
   protected Collection<Address> getInvalidHolders(Object key, ConsistentHash chOld, ConsistentHash chNew) {
      Collection<Address> l = super.getInvalidHolders(key, chOld, chNew);
      l.removeAll(leaversHandled);
      return l;
   }
}

abstract class StateMap<S> {
   List<Address> leavers;
   ConsistentHash oldCH, newCH;
   int replCount;
   Map<Address, S> state = new HashMap<Address, S>();

   StateMap(List<Address> leavers, ConsistentHash oldCH, ConsistentHash newCH, int replCount) {
      this.leavers = leavers;
      this.oldCH = oldCH;
      this.newCH = newCH;
      this.replCount = replCount;
   }

   Map<Address, S> getState() {
      return state;
   }
}

class InMemoryStateMap extends StateMap<Map<Object, InternalCacheValue>> {
   Address self;
   Set<Object> keysHandled = new HashSet<Object>();

   InMemoryStateMap(List<Address> leavers, Address self, ConsistentHash oldCH, ConsistentHash newCH, int replCount) {
      super(leavers, oldCH, newCH, replCount);
      this.self = self;
   }

   /**
    * Only add state to state map if old_owner_list for key contains a leaver, and the position of the leaver in the old
    * owner list
    *
    * @param payload an InternalCacheEntry to add to the state map
    */
   void addState(InternalCacheEntry payload) {
      Object key = payload.getKey();
      for (Address leaver : leavers) {
         List<Address> owners = oldCH.locate(key, replCount);
         int leaverIndex = owners.indexOf(leaver);
         if (leaverIndex > -1) {
            int numOwners = owners.size();
            int selfIndex = owners.indexOf(self);
            boolean isLeaverLast = leaverIndex == numOwners - 1;
            if ((isLeaverLast && selfIndex == numOwners - 2) ||
                  (!isLeaverLast && selfIndex == leaverIndex + 1)) {
               // add to state map!
               List<Address> newOwners = newCH.locate(key, replCount);
               newOwners.removeAll(owners);
               if (!newOwners.isEmpty()) {
                  for (Address no : newOwners) {
                     Map<Object, InternalCacheValue> s = state.get(no);
                     if (s == null) {
                        s = new HashMap<Object, InternalCacheValue>();
                        state.put(no, s);
                     }
                     s.put(key, payload.toInternalCacheValue());
                  }
               }
            }
         }
      }
      keysHandled.add(key);
   }

   boolean doesNotContainKey(Object key) {
      return !keysHandled.contains(key);
   }
}

/**
 * A state map that aggregates {@link ReplicableCommand}s according to recipient affected.
 *
 * @param <T> type of replicable command to aggregate
 */
abstract class CommandAggregatingStateMap<T extends ReplicableCommand> extends StateMap<List<T>> {
   Set<Object> keysHandled = new HashSet<Object>();

   CommandAggregatingStateMap(List<Address> leavers, ConsistentHash oldCH, ConsistentHash newCH, int replCount) {
      super(leavers, oldCH, newCH, replCount);
   }

   // if only Java had duck-typing!
   abstract Set<Object> getAffectedKeys(T payload);

   /**
    * Only add state to state map if old_owner_list for key contains a leaver, and the position of the leaver in the old
    * owner list
    *
    * @param payload payload to consider when adding to the aggregate state
    */
   void addState(T payload) {
      for (Object key : getAffectedKeys(payload)) {
         for (Address leaver : leavers) {
            List<Address> owners = oldCH.locate(key, replCount);
            int leaverIndex = owners.indexOf(leaver);
            if (leaverIndex > -1) {
               // add to state map!
               List<Address> newOwners = newCH.locate(key, replCount);
               newOwners.removeAll(owners);
               if (!newOwners.isEmpty()) {
                  for (Address no : newOwners) {
                     List<T> s = state.get(no);
                     if (s == null) {
                        s = new LinkedList<T>();
                        state.put(no, s);
                     }
                     s.add(payload);
                  }
               }
            }
         }
      }
   }
}

/**
 * Specific version of the CommandAggregatingStateMap that aggregates PrepareCommands, used to flush pending prepares
 * to nodes during a leave.
 */
class PendingPreparesMap extends CommandAggregatingStateMap<PrepareCommand> {
   PendingPreparesMap(List<Address> leavers, ConsistentHash oldCH, ConsistentHash newCH, int replCount) {
      super(leavers, oldCH, newCH, replCount);
   }

   @Override
   Set<Object> getAffectedKeys(PrepareCommand payload) {
      return payload.getAffectedKeys();
   }
}

/**
 * Specific version of the CommandAggregatingStateMap that aggregates PrepareCommands, used to flush writes
 * made while state was being transferred to nodes during a leave. 
 */
class TransactionLogMap extends CommandAggregatingStateMap<WriteCommand> {
   TransactionLogMap(List<Address> leavers, ConsistentHash oldCH, ConsistentHash newCH, int replCount) {
      super(leavers, oldCH, newCH, replCount);
   }

   @Override
   Set<Object> getAffectedKeys(WriteCommand payload) {
      return payload.getAffectedKeys();
   }
}