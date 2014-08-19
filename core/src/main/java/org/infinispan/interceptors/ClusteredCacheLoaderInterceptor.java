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

/**
 * The same as a regular cache loader interceptor, except that it contains additional logic to force loading from the
 * cache loader if needed on a remote node, in certain conditions.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ClusteredCacheLoaderInterceptor extends CacheLoaderInterceptor {

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
   private void startClusteredCacheLoaderInterceptor() {
      transactional = cacheConfiguration.transaction().transactionMode().isTransactional();
      distributed = cacheConfiguration.clustering().cacheMode().isDistributed();
   }

   @Override
   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      return transactional ? skipLoadForTxCommand(cmd, key, ctx) : skipLoadForNonTxCommand(cmd, key);
   }

   private boolean skipLoadForNonTxCommand(WriteCommand cmd, Object key) {
      if (cdl.localNodeIsPrimaryOwner(key) || cmd.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (isDeltaWrite(cmd)) {
            if (trace) {
               log.tracef("Don't skip load for DeltaWrite or conditional command %s.", cmd);
            }
            return false;
         } else if (isConditional(cmd)) {
            boolean skip = hasSkipLoadFlag(cmd);
            if (trace) {
               log.tracef("Skip load for conditional command %s? %s", cmd, skip);
            }
            return skip;
         }
         boolean skip = hasSkipLoadFlag(cmd) || hasIgnoreReturnValueFlag(cmd);
         if (trace) {
            log.tracef("Skip load for command %s? %s", skip);
         }
         return skip;
      }
      if (trace) {
         log.tracef("Skip load for command %s. This node is not the primary owner of %s", cmd, key);
      }
      return true;
   }

   private boolean skipLoadForTxCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      if (isDeltaWrite(cmd)) {
         if (trace) {
            log.tracef("Don't skip load for DeltaWrite command %s.", cmd);
         }
         return false;
      } else if (isConditional(cmd)) {
         boolean skip = hasSkipLoadFlag(cmd);
         if (trace) {
            log.tracef("Skip load for conditional command %s? %s", cmd, skip);
         }
         return skip;
      }
      boolean skip = hasSkipLoadFlag(cmd) || hasIgnoreReturnValueFlag(cmd);
      if (trace) {
         log.tracef("Skip load for command %s? %s", skip);
      }
      return skip || (!ctx.isOriginLocal() && !cdl.localNodeIsPrimaryOwner(key));
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
