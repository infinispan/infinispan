package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.remoting.rpc.ResponseMode.SYNCHRONOUS;

/**
 * A task that handles the rehashing of data in the cache system wheh nodes join or leave the cluster.  This abstract
 * class contains common functionality.  Subclasses will specify different behavior for nodes joining and leaving.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class RehashTask implements Callable<Void> {
   protected DistributionManager distributionManager;
   protected RpcManager rpcManager;
   protected Configuration configuration;
   protected CommandsFactory cf;
   protected DataContainer dataContainer;
   protected final Address self;
   private final AtomicInteger counter = new AtomicInteger(0);
   protected final Log log = LogFactory.getLog(getClass());
   protected final boolean trace = log.isTraceEnabled();

   protected final ExecutorService statePullExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            Thread th = new Thread(r, "Rehasher-" + self + "-Worker-" + counter.getAndIncrement());
            th.setDaemon(true);
            return th;
         }
      });


   protected RehashTask(DistributionManagerImpl distributionManager, RpcManager rpcManager,
            Configuration configuration, CommandsFactory cf, DataContainer dataContainer) {
      this.distributionManager = distributionManager;
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.cf = cf;
      this.dataContainer = dataContainer;
      this.self = rpcManager.getAddress();
   }

   public Void call() throws Exception {
      distributionManager.setRehashInProgress(true);
      try {
         performRehash();
         return null;
      } finally {
         distributionManager.setRehashInProgress(false);
      }
   }

   protected abstract void performRehash() throws Exception;

   protected Collection<Address> coordinator() {
      return Collections.singleton(rpcManager.getTransport().getCoordinator());
   }

   protected void invalidateInvalidHolders(List<Address> doNotInvalidate, ConsistentHash chOld, ConsistentHash chNew) throws ExecutionException, InterruptedException {
      if (log.isDebugEnabled()) log.debug("Invalidating entries that have migrated across");
      Map<Address, Set<Object>> invalidations = new HashMap<Address, Set<Object>>();
      for (Object key : dataContainer.keySet()) {
         Collection<Address> invalidHolders = getInvalidHolders(key, chOld, chNew);
         for (Address a : invalidHolders) {
            Set<Object> s = invalidations.get(a);
            if (s == null) {
               s = new HashSet<Object>();
               invalidations.put(a, s);
            }
            s.add(key);
         }
      }

      invalidations.keySet().removeAll(doNotInvalidate);

      for (Map.Entry<Address, Set<Object>> e : invalidations.entrySet()) {
         InvalidateCommand ic = cf.buildInvalidateFromL1Command(true, e.getValue().toArray());
         rpcManager.invokeRemotely(Collections.singletonList(e.getKey()), ic, false);
      }
   }
   protected void invalidateInvalidHolders(ConsistentHash chOld, ConsistentHash chNew) throws ExecutionException, InterruptedException {
      List<Address> none = Collections.emptyList();
      invalidateInvalidHolders(none, chOld, chNew);
   }

   protected Collection<Address> getInvalidHolders(Object key, ConsistentHash chOld, ConsistentHash chNew) {
      List<Address> oldOwners = chOld.locate(key, configuration.getNumOwners());
      List<Address> newOwners = chNew.locate(key, configuration.getNumOwners());

      List<Address> toInvalidate = new LinkedList<Address>(oldOwners);
      toInvalidate.removeAll(newOwners);

      return toInvalidate;
   }

   protected abstract class StateGrabber implements Callable<Void> {
      private final Address stateProvider;
      private final ReplicableCommand command;
      private final ConsistentHash newConsistentHash;

      public StateGrabber(Address stateProvider, ReplicableCommand command, ConsistentHash newConsistentHash) {
         this.stateProvider = stateProvider;
         this.command = command;
         this.newConsistentHash = newConsistentHash;
      }

      @Override
      public Void call() throws Exception {
         // This call will cause the sender to start logging transactions - BEFORE generating state.
         Map<Address, Response> resps = rpcManager.invokeRemotely(Collections.singleton(stateProvider), command, SYNCHRONOUS, configuration.getRehashRpcTimeout(), true);
         for (Response r : resps.values()) {
            if (r instanceof SuccessfulResponse) {
               Map<Object, InternalCacheValue> state = getStateFromResponse((SuccessfulResponse) r);
               distributionManager.applyState(newConsistentHash, state, new RemoteTransactionLoggerImpl(cf, stateProvider, rpcManager), isForLeave());
            }
         }

         return null;
      }

      protected abstract boolean isForLeave();

      @SuppressWarnings("unchecked")
      private Map<Object, InternalCacheValue> getStateFromResponse(SuccessfulResponse r) {
         return (Map<Object, InternalCacheValue>) r.getResponseValue();
      }
   }
}
