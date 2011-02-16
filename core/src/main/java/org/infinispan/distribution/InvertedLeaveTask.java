package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
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

import static java.lang.System.currentTimeMillis;
import static org.infinispan.commands.control.RehashControlCommand.Type.LEAVE_REHASH_END;
import static org.infinispan.commands.control.RehashControlCommand.Type.PULL_STATE_LEAVE;
import static org.infinispan.remoting.rpc.ResponseMode.SYNCHRONOUS;
import static org.infinispan.util.Util.prettyPrintTime;

/**
 * A task to handle rehashing for when a node leaves the cluster
 *
 * @author Vladimir Blagojevic
 * @author Manik Surtani
 * @since 4.0
 */
public class InvertedLeaveTask extends RehashTask {
   private final DistributionManagerImpl distributionManager;
   private final List<Address> leaversHandled;
   private final List<Address> providers;
   private final List<Address> receivers;
   private final boolean isReceiver;
   private final boolean isSender;

   public InvertedLeaveTask(DistributionManagerImpl dmi, RpcManager rpcManager, Configuration conf,
                            CommandsFactory commandsFactory, DataContainer dataContainer,
                            List<Address> stateProviders, List<Address> stateReceivers, boolean isReceiver) {
      super(dmi, rpcManager, conf, commandsFactory, dataContainer);
      this.distributionManager = dmi;
      this.leaversHandled = new LinkedList<Address>(distributionManager.getLeavers());
      this.providers = stateProviders;
      this.receivers = stateReceivers;
      this.isReceiver = isReceiver;
      this.isSender = stateProviders.contains(self);
   }

   protected void performRehash() throws Exception {
      long start = currentTimeMillis();

      int replCount = configuration.getNumOwners();
      ConsistentHash newCH = this.distributionManager.getConsistentHash();
      ConsistentHash oldCH = ConsistentHashHelper.createConsistentHash(configuration, newCH.getCaches(), leaversHandled, this.distributionManager.topologyInfo);

      try {
         log.debug("Starting leave rehash[enabled=%s,isReceiver=%s,isSender=%s] on node %s", configuration.isRehashEnabled(), isReceiver, isSender, self);
         // Handling leaves are tough.  We need to prevent any LOCAL transactions from running, if we are a receiver
         // of state.
         if (isReceiver) this.distributionManager.getTransactionLogger().blockNewTransactions();


         if (configuration.isRehashEnabled()) {
            if (isReceiver) {
               // optimise ...
               providers.remove(self);

               try {
                  RehashControlCommand cmd = cf.buildRehashControlCommand(PULL_STATE_LEAVE, self,
                                                                          null, oldCH, newCH, leaversHandled);

                  log.debug("I %s am pulling state from %s", self, providers);
                  Set<Future<Void>> stateRetrievalProcesses = new HashSet<Future<Void>>(providers.size());
                  for (Address stateProvider : providers) {
                     stateRetrievalProcesses.add(
                           statePullExecutor.submit(new LeaveStateGrabber(stateProvider, cmd, newCH))
                     );
                  }

                  // Wait for all this state to be applied, in parallel.
                  log.trace("State retrieval being processed.");
                  for (Future<Void> f : stateRetrievalProcesses) f.get();
                  log.trace("State retrieval from %s completed.", providers);

               } finally {
                  // Inform state senders that state has been applied successfully so they can proceed.
                  // Needs to be SYNC - we need to make sure these messages don't get 'lost' or you end up with a
                  // blocked up cluster
                  log.trace("Informing %s that state has been applied.", providers);
                  RehashControlCommand c = cf.buildRehashControlCommand(LEAVE_REHASH_END, self);
                  rpcManager.invokeRemotely(providers, c, SYNCHRONOUS, configuration.getRehashRpcTimeout(), true);
               }
            }
            if (isSender) {
               Set<Address> recCopy = new HashSet<Address>(receivers);
               recCopy.remove(self);

               this.distributionManager.awaitLeaveRehashAcks(recCopy, configuration.getStateRetrievalTimeout());
               processAndDrainTxLog(oldCH, newCH, replCount);
               invalidateInvalidHolders(leaversHandled, oldCH, newCH);
            }
         }
      } catch (Exception e) {
         throw new CacheException("Unexpected exception", e);
      } finally {
         for (Address addr : leaversHandled) this.distributionManager.markLeaverAsHandled(addr);
         if (isReceiver) this.distributionManager.getTransactionLogger().unblockNewTransactions();
         log.info("Completed leave rehash on node %s in %s - leavers now are %s", self, prettyPrintTime(currentTimeMillis() - start), this.distributionManager.leavers);
      }
   }


   private void processAndDrainTxLog(ConsistentHash oldCH, ConsistentHash newCH, int replCount) {

      List<WriteCommand> c;
      int i = 0;
      TransactionLogger transactionLogger = this.distributionManager.getTransactionLogger();
      if (trace)
         log.trace("Processing transaction log iteratively: " + transactionLogger);
      while (transactionLogger.shouldDrainWithoutLock()) {
         if (trace)
            log.trace("Processing transaction log, iteration %s", i++);
         c = transactionLogger.drain();
         if (trace)
            log.trace("Found %s modifications", c.size());
         apply(oldCH, newCH, replCount, c);
      }

      if (trace)
         log.trace("Processing transaction log: final drain and lock");
      c = transactionLogger.drainAndLock(null);
      if (trace)
         log.trace("Found %s modifications", c.size());
      apply(oldCH, newCH, replCount, c);

      if (trace)
         log.trace("Handling pending prepares");
      PendingPreparesMap state = new PendingPreparesMap(Collections.unmodifiableList(distributionManager.getLeavers()), oldCH, newCH, replCount);
      Collection<PrepareCommand> pendingPrepares = transactionLogger.getPendingPrepares();
      if (trace)
         log.trace("Found %s pending prepares", pendingPrepares.size());
      for (PrepareCommand pc : pendingPrepares)
         state.addState(pc);

      if (trace)
         log.trace("State map for pending prepares is %s", state.getState());

      Set<Future<Object>> pushFutures = new HashSet<Future<Object>>();
      for (Map.Entry<Address, List<PrepareCommand>> e : state.getState().entrySet()) {
         if (log.isDebugEnabled())
            log.debug("Pushing %s uncommitted prepares to %s", e.getValue().size(), e.getKey());
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

      transactionLogger.unlockAndDisable(null);
   }

   private void apply(ConsistentHash oldCH, ConsistentHash newCH, int replCount, List<WriteCommand> wc) {
      // need to create another "state map"
      TransactionLogMap state = new TransactionLogMap(Collections.unmodifiableList(distributionManager.getLeavers()), oldCH, newCH, replCount);
      for (WriteCommand c : wc)
         state.addState(c);

      if (trace)
         log.trace("State map for modifications is %s", state.getState());

      Set<Future<Object>> pushFutures = new HashSet<Future<Object>>();
      for (Map.Entry<Address, List<WriteCommand>> entry : state.getState().entrySet()) {
         if (log.isDebugEnabled())
            log.debug("Pushing %s modifications to %s", entry.getValue().size(), entry.getKey());
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

   protected final class LeaveStateGrabber extends StateGrabber {

      public LeaveStateGrabber(Address stateProvider, ReplicableCommand command, ConsistentHash newConsistentHash) {
         super(stateProvider, command, newConsistentHash);
      }

      @Override
      protected boolean isForLeave() {
         return true;
      }
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
 * Specific version of the CommandAggregatingStateMap that aggregates PrepareCommands, used to flush pending prepares to
 * nodes during a leave.
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
 * Specific version of the CommandAggregatingStateMap that aggregates PrepareCommands, used to flush writes made while
 * state was being transferred to nodes during a leave.
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
