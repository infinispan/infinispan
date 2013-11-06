package org.infinispan.util;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.remoting.rpc.RpcManager;

/**
 * Use the {@link CountingRpcManager#replaceRpcManager(org.infinispan.Cache)}.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class CountingRpcManager extends AbstractControlledRpcManager {

   public volatile int lockCount;
   public volatile int clusterGet;
   public volatile int otherCount;

   public CountingRpcManager(RpcManager realOne) {
      super(realOne);
   }

   public static CountingRpcManager replaceRpcManager(Cache c) {
      AdvancedCache advancedCache = c.getAdvancedCache();
      CountingRpcManager crm = new CountingRpcManager(advancedCache.getRpcManager());
      advancedCache.getComponentRegistry().registerComponent(crm, RpcManager.class);
      advancedCache.getComponentRegistry().rewire();
      assert advancedCache.getRpcManager().equals(crm);
      return crm;
   }

   public void resetStats() {
      lockCount = 0;
      clusterGet = 0;
      otherCount = 0;
   }

   @Override
   protected void beforeInvokeRemotely(ReplicableCommand rpcCommand) {
      if (rpcCommand instanceof LockControlCommand) {
         lockCount++;
      } else if (rpcCommand instanceof ClusteredGetCommand) {
         clusterGet++;
      } else {
         otherCount++;
      }
   }
}
