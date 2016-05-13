package org.infinispan.interceptors;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.commons.util.Util.toStr;

/**
 * The same as a regular cache loader interceptor, except that it contains additional logic to force loading from the
 * cache loader if needed on a remote node, in certain conditions.
 *
 * @author Manik Surtani
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class ClusteredActivationInterceptor extends ActivationInterceptor {

   private static final Log log = LogFactory.getLog(ClusteredActivationInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private boolean transactional;
   private ClusteringDependentLogic cdl;
   private StateTransferManager stateTransferManager;
   private boolean distributed;

   @Inject
   private void injectDependencies(ClusteringDependentLogic cdl, StateTransferManager stateTransferManager) {
      this.cdl = cdl;
      this.stateTransferManager = stateTransferManager;
   }

   @Start(priority = 15)
   private void startClusteredActivationInterceptor() {
      transactional = cacheConfiguration.transaction().transactionMode().isTransactional();
      distributed = cacheConfiguration.clustering().cacheMode().isDistributed();
   }

   @Override
   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      if (!cmd.alwaysReadsExistingValues()) {
         if (transactional) {
            if (!ctx.isOriginLocal()) {
               if (trace) log.tracef("Skip load for remote tx write command %s.", cmd);
               return true;
            }
         } else {
            if (!cdl.localNodeIsPrimaryOwner(key) && !cmd.hasFlag(Flag.CACHE_MODE_LOCAL)) {
               if (trace) {
                  log.tracef("Skip load for command %s. This node is not the primary owner of %s", cmd, toStr(key));
               }
               return true;
            }
         }
      }
      return super.skipLoadForWriteCommand(cmd, key, ctx);
   }

   @Override
   protected boolean canLoad(Object key) {
      // Don't load the value if we are using distributed mode and aren't in the read CH
      return stateTransferManager.isJoinComplete() && (!distributed || isKeyLocal(key));
   }

   private boolean isKeyLocal(Object key) {
      return stateTransferManager.getCacheTopology().getReadConsistentHash().isKeyLocalToNode(cdl.getAddress(), key);
   }
}
