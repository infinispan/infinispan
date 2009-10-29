package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

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
import java.util.concurrent.Future;

/**
 * A task that handles the rehashing of data in the cache system wheh nodes join or leave the cluster.  This abstract
 * class contains common functionality.  Subclasses will specify different behavior for nodes joining and leaving.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class RehashTask implements Callable<Void> {

   DistributionManagerImpl dmi;
   RpcManager rpcManager;
   Configuration configuration;
   TransactionLogger transactionLogger;
   CommandsFactory cf;
   DataContainer dataContainer;

   protected RehashTask(DistributionManagerImpl dmi, RpcManager rpcManager, Configuration configuration,
                        TransactionLogger transactionLogger, CommandsFactory cf, DataContainer dataContainer) {
      this.dmi = dmi;
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.transactionLogger = transactionLogger;
      this.cf = cf;
      this.dataContainer = dataContainer;
   }

   public Void call() throws Exception {
      dmi.rehashInProgress = true;
      try {
         performRehash();
         return null;
      } finally {
         dmi.rehashInProgress = false;
      }
   }

   protected abstract void performRehash() throws Exception;

   protected Collection<Address> coordinator() {
      return Collections.singleton(rpcManager.getTransport().getCoordinator());
   }

   protected void invalidateInvalidHolders(ConsistentHash chOld, ConsistentHash chNew) throws ExecutionException, InterruptedException {
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

      Set<Future> futures = new HashSet<Future>();

      for (Map.Entry<Address, Set<Object>> e : invalidations.entrySet()) {
         InvalidateCommand ic = cf.buildInvalidateFromL1Command(e.getValue().toArray());
         NotifyingNotifiableFuture f = new NotifyingFutureImpl(null);
         rpcManager.invokeRemotelyInFuture(Collections.singletonList(e.getKey()), ic, true, f);
         futures.add(f);
      }

      for (Future f : futures) f.get();
   }

   protected Collection<Address> getInvalidHolders(Object key, ConsistentHash chOld, ConsistentHash chNew) {
      List<Address> oldOwners = chOld.locate(key, configuration.getNumOwners());
      List<Address> newOwners = chNew.locate(key, configuration.getNumOwners());

      List<Address> toInvalidate = new LinkedList<Address>(oldOwners);
      toInvalidate.removeAll(newOwners);

      return toInvalidate;
   }
}
