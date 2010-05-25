package org.infinispan.distribution;

import static org.infinispan.commands.control.RehashControlCommand.Type.PULL_STATE_LEAVE;
import static org.infinispan.remoting.rpc.ResponseMode.SYNCHRONOUS;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
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
   private static final boolean trace = true;
   private final List<Address> leavers;
   private final Address self;
   private final List<Address> leaversHandled;
   private final List<Address> stateProviders;
   private final boolean isReceiver;

   public InvertedLeaveTask(DistributionManagerImpl dmi, RpcManager rpcManager, Configuration conf,
            CommandsFactory commandsFactory, DataContainer dataContainer, List<Address> leavers,
            List<Address> stateProviders, boolean isReceiver) {
      super(dmi, rpcManager, conf, commandsFactory, dataContainer);
      this.leavers = leavers;
      this.leaversHandled = new LinkedList<Address>(leavers);
      this.stateProviders = stateProviders;
      this.isReceiver = isReceiver;
      this.self = rpcManager.getTransport().getAddress();
   }

   @SuppressWarnings("unchecked")
   private Map<Object, InternalCacheValue> getStateFromResponse(SuccessfulResponse r) {
      return (Map<Object, InternalCacheValue>) r.getResponseValue();
   }

   protected void performRehash() throws Exception {
      long start = System.currentTimeMillis();
      boolean trace = log.isTraceEnabled();

      int replCount = configuration.getNumOwners();
      ConsistentHash oldCH = ConsistentHashHelper.createConsistentHash(configuration, dmi.getConsistentHash().getCaches(), leaversHandled);
      ConsistentHash newCH = dmi.getConsistentHash();
      try {
         if (log.isDebugEnabled()) {
            if (isReceiver) {
               log.debug("Commencing rehash at {0}, I am a state receiver", self);
            } else {
               log.debug("Commencing rehash at {0}, I am a state producer", self);
            }
         }
         if (configuration.isRehashEnabled()) {
            if (isReceiver) {
               Address myAddress = rpcManager.getTransport().getAddress();
               RehashControlCommand cmd = cf.buildRehashControlCommand(PULL_STATE_LEAVE, myAddress,
                        null, oldCH, newCH,leaversHandled);

               List<Address> addressesWhoMaySendStuff = getStateProviderTargets();
               log.debug("I {0} am pulling state from {1}", self, addressesWhoMaySendStuff);
               List<Response> resps = rpcManager.invokeRemotely(addressesWhoMaySendStuff, cmd,
                        SYNCHRONOUS, configuration.getRehashRpcTimeout(), true);

               log.debug("I {0} received response {1} ", self, resps);
               for (Response r : resps) {
                  if (r instanceof SuccessfulResponse) {
                     Map<Object, InternalCacheValue> state = getStateFromResponse((SuccessfulResponse) r);
                     log.debug("I {0} am applying state {1} ", self, state);
                     dmi.applyState(newCH, state);
                  }
               }
            }
            processAndDrainTxLog(oldCH, newCH, replCount);
            invalidateInvalidHolders(leaversHandled, oldCH, newCH);
         } else {
            if (trace)
               log.trace("Rehash not enabled, so not pulling state.");
         }
         if (trace)
            log.info("{0} completed leave rehash in {1}!", self, Util.prettyPrintTime(System.currentTimeMillis()
                     - start));
      } catch (Exception e) {        
         throw new CacheException("Unexpected exception", e);
      } finally {
         leavers.removeAll(leaversHandled);
      }
   }

   private List<Address> getStateProviderTargets() {
      return stateProviders;
   }

   private void processAndDrainTxLog(ConsistentHash oldCH, ConsistentHash newCH, int replCount) {
      if (trace)
         log.trace("Processing transaction log iteratively");

      List<WriteCommand> c;
      int i = 0;
      TransactionLogger transactionLogger = dmi.getTransactionLogger();
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

   private void apply(ConsistentHash oldCH, ConsistentHash newCH, int replCount,
            List<WriteCommand> wc) {
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
