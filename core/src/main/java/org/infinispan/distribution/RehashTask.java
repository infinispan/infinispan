package org.infinispan.distribution;

import org.infinispan.config.Configuration;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class RehashTask implements Callable<Void> {

   DistributionManagerImpl dmi;
   RpcManager rpcManager;
   Configuration configuration;

   protected RehashTask(DistributionManagerImpl dmi, RpcManager rpcManager, Configuration configuration) {
      this.dmi = dmi;
      this.rpcManager = rpcManager;
      this.configuration = configuration;
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

   protected ConsistentHash createConsistentHash(Collection<Address> addresses) throws Exception {
      ConsistentHash ch = (ConsistentHash) Util.getInstance(configuration.getConsistentHashClass());
      ch.setCaches(addresses);
      return ch;
   }

   protected ConsistentHash createConsistentHash(Collection<Address> addresses, Address... moreAddresses) throws Exception {
      List<Address> list = new LinkedList<Address>(addresses);
      list.addAll(Arrays.asList(moreAddresses));
      return createConsistentHash(list);
   }
}
