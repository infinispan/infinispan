package org.infinispan.distribution;

import static org.infinispan.commands.control.RehashControlCommand.Type.PULL_STATE_LEAVE;
import static org.infinispan.commands.control.RehashControlCommand.Type.LEAVE_REHASH_END;

import static org.infinispan.remoting.rpc.ResponseMode.SYNCHRONOUS;
import static org.infinispan.remoting.rpc.ResponseMode.ASYNCHRONOUS;

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

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 *A task to handle rehashing for when a node leaves the cluster
 * 
 * @author Vladimir Blagojevic
 * @author Manik Surtani
 * @since 4.0
 */
public class InvertedLeaveTask extends RehashTask {

   private static final Log log = LogFactory.getLog(InvertedLeaveTask.class);
   private static final boolean trace = log.isTraceEnabled();
   private final List<Address> leavers;
   private final Address self;
   private final List<Address> leaversHandled;
   private final List<Address> providers;
   private final List<Address> receivers;
   private final boolean isReceiver;
   private final boolean isSender;

   public InvertedLeaveTask(DistributionManagerImpl dmi, RpcManager rpcManager, Configuration conf,
            CommandsFactory commandsFactory, DataContainer dataContainer, List<Address> leavers,
            List<Address> stateProviders, List<Address> stateReceivers, boolean isReceiver) {
      super(dmi, rpcManager, conf, commandsFactory, dataContainer);
      this.leavers = leavers;
      this.leaversHandled = new LinkedList<Address>(leavers);
      this.providers = stateProviders;
      this.receivers = stateReceivers;
      this.isReceiver = isReceiver;      
      this.self = rpcManager.getTransport().getAddress();
      this.isSender = stateProviders.contains(self);
   }

   @SuppressWarnings("unchecked")
   private Map<Object, InternalCacheValue> getStateFromResponse(SuccessfulResponse r) {
      return (Map<Object, InternalCacheValue>) r.getResponseValue();
   }

   protected void performRehash() throws Exception {
      long start = trace ? System.currentTimeMillis() : 0;

      int replCount = configuration.getNumOwners();
      ConsistentHash newCH = dmi.getConsistentHash();
      ConsistentHash oldCH = ConsistentHashHelper.createConsistentHash(configuration, newCH.getCaches(), leaversHandled, dmi.topologyInfo);

      try {
         log.debug("Starting leave rehash[enabled={0},isReceiver={1},isSender={2}] on node {3}",
                  configuration.isRehashEnabled(), isReceiver, isSender, self);

         if (configuration.isRehashEnabled()) {
            if (isReceiver) {
               try {
                  RehashControlCommand cmd = cf.buildRehashControlCommand(PULL_STATE_LEAVE, self,
                           null, oldCH, newCH, leaversHandled);

                  log.debug("I {0} am pulling state from {1}", self, providers);
                  List<Response> resps = rpcManager.invokeRemotely(providers, cmd, SYNCHRONOUS,
                           configuration.getRehashRpcTimeout(), true);

                  log.debug("I {0} received response {1} ", self, resps);
                  for (Response r : resps) {
                     if (r instanceof SuccessfulResponse) {
                        Map<Object, InternalCacheValue> state = getStateFromResponse((SuccessfulResponse) r);
                        log.debug("I {0} am applying state {1} ", self, state);
                        dmi.applyState(newCH, state);
                     }
                  }
               } finally {
                  RehashControlCommand c = cf.buildRehashControlCommand(LEAVE_REHASH_END, self);
                  rpcManager.invokeRemotely(providers, c, ASYNCHRONOUS, configuration.getRehashRpcTimeout(), false);
               }
            }
            if (isSender) {
               dmi.awaitLeaveRehashAcks(receivers, configuration.getStateRetrievalTimeout());
               processAndDrainTxLog(oldCH, newCH, replCount);
               invalidateInvalidHolders(leaversHandled, oldCH, newCH);
            }
         }
      } catch (Exception e) {
         throw new CacheException("Unexpected exception", e);
      } finally {
         leavers.removeAll(leaversHandled);
         if (trace)
            log.info("Completed leave rehash on node {0} in {1}", self,
                     Util.prettyPrintTime(System.currentTimeMillis() - start));
         else
            log.info("Completed leave rehash on node {0}", self);

         for (Address addr : leaversHandled)
            dmi.topologyInfo.removeNodeInfo(addr);
      }
   }

   private void processAndDrainTxLog(ConsistentHash oldCH, ConsistentHash newCH, int replCount) {

      List<WriteCommand> c;
      int i = 0;
      TransactionLogger transactionLogger = dmi.getTransactionLogger();
      if (trace)
         log.trace("Processing transaction log iteratively: " + transactionLogger);
      while (transactionLogger.shouldDrainWithoutLock()) {
         if (trace)
            log.trace("Processing transaction log, iteration {0}", i++);
         c = transactionLogger.drain();
         if (trace)
            log.trace("Found {0} modifications", c.size());
         apply(oldCH, newCH, replCount, c);
      }

      if (trace)
         log.trace("Processing transaction log: final drain and lock");
      c = transactionLogger.drainAndLock();
      if (trace)
         log.trace("Found {0} modifications", c.size());
      apply(oldCH, newCH, replCount, c);

      if (trace)
         log.trace("Handling pending prepares");
      PendingPreparesMap state = new PendingPreparesMap(leavers, oldCH, newCH, replCount);
      Collection<PrepareCommand> pendingPrepares = transactionLogger.getPendingPrepares();
      if (trace)
         log.trace("Found {0} pending prepares", pendingPrepares.size());
      for (PrepareCommand pc : pendingPrepares)
         state.addState(pc);

      if (trace)
         log.trace("State map for pending prepares is {0}", state.getState());

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
      if (trace)
         log.trace("Finished pushing pending prepares; unlocking and disabling transaction logging");

      transactionLogger.unlockAndDisable();
   }

   private void apply(ConsistentHash oldCH, ConsistentHash newCH, int replCount, List<WriteCommand> wc) {
      // need to create another "state map"
      TransactionLogMap state = new TransactionLogMap(leavers, oldCH, newCH, replCount);
      for (WriteCommand c : wc)
         state.addState(c);

      if (trace)
         log.trace("State map for modifications is {0}", state.getState());

      Set<Future<Object>> pushFutures = new HashSet<Future<Object>>();
      for (Map.Entry<Address, List<WriteCommand>> entry : state.getState().entrySet()) {
         if (log.isDebugEnabled())
            log.debug("Pushing {0} modifications to {1}", entry.getValue().size(), entry.getKey());
         RehashControlCommand push = cf.buildRehashControlCommandTxLog(self, entry.getValue());
         NotifyingNotifiableFuture<Object> f = new NotifyingFutureImpl(null);
         pushFutures.add(f);
         rpcManager.invokeRemotelyInFuture(Collections.singleton(entry.getKey()), push, true, f,
                  configuration.getRehashRpcTimeout());
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
   protected Log getLog() {
      return log;
   }
}

abstract class StateMap<S> {
   List<Address> leavers;
   ConsistentHash oldCH, newCH;
   int replCount;
   Map<Address, S> state = new HashMap<Address, S>();
   protected static final Log log = LogFactory.getLog(InvertedLeaveTask.class);

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
