package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
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
 * @since 9.0
 */
public class ClusteredActivationInterceptor extends ActivationInterceptor {

   private static final Log log = LogFactory.getLog(ClusteredActivationInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private boolean transactional;
   /**
    * Variable that defines whether or not a cache store load is required on an entry create in the context
    * This is required when RepeatableRead is enabled with WSC
    */
   private boolean forceReadOnCreate;
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
      // If write skew is enabled on total order we have to read the entry on a create - in case if it is in the loader
      forceReadOnCreate = transactional && cacheConfiguration.transaction().transactionProtocol().isTotalOrder() &&
            cacheConfiguration.locking().writeSkewCheck();
      distributed = cacheConfiguration.clustering().cacheMode().isDistributed();
   }

   @Override
   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      if (!cmd.alwaysReadsExistingValues()) {
         if (transactional) {
            if (!ctx.isOriginLocal()) {
               CacheEntry entry;
               if (!forceReadOnCreate || ((entry = ctx.lookupEntry(key)) != null && !entry.isCreated())) {
                  if (trace) log.tracef("Skip load for remote tx write command %s.", cmd);
                  return true;
               }
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
