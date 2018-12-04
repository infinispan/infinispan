package org.infinispan.util;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;

/**
 * Use the {@link CountingRpcManager#replaceRpcManager(org.infinispan.Cache)}.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class CountingRpcManager extends AbstractDelegatingRpcManager {

   public volatile int lockCount;
   public volatile int clusterGet;
   public volatile int otherCount;

   public CountingRpcManager(RpcManager realOne) {
      super(realOne);
   }

   public static CountingRpcManager replaceRpcManager(Cache c) {
      AdvancedCache advancedCache = c.getAdvancedCache();
      CountingRpcManager crm = new CountingRpcManager(advancedCache.getRpcManager());
      BasicComponentRegistry bcr = advancedCache.getComponentRegistry().getComponent(BasicComponentRegistry.class);
      bcr.replaceComponent(RpcManager.class.getName(), crm, false);
      bcr.rewire();
      assert advancedCache.getRpcManager().equals(crm);
      return crm;
   }

   public void resetStats() {
      lockCount = 0;
      clusterGet = 0;
      otherCount = 0;
   }

   @Override
   protected <T> CompletionStage<T> performRequest(Collection<Address> targets, ReplicableCommand command,
                                                   ResponseCollector<T> collector,
                                                   Function<ResponseCollector<T>, CompletionStage<T>> invoker,
                                                   RpcOptions rpcOptions) {
      if (command instanceof LockControlCommand) {
         lockCount++;
      } else if (command instanceof ClusteredGetCommand) {
         clusterGet++;
      } else {
         otherCount++;
      }
      return super.performRequest(targets, command, collector, invoker, rpcOptions);
   }
}
