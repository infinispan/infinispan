package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
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
public class ClusteredCacheLoaderInterceptor extends CacheLoaderInterceptor {

   private static final Log log = LogFactory.getLog(ClusteredCacheLoaderInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private StateTransferManager stateTransferManager;
   @Inject private DistributionManager distributionManager;

   private boolean transactional;

   @Start(priority = 15)
   private void startClusteredCacheLoaderInterceptor() {
      transactional = cacheConfiguration.transaction().transactionMode().isTransactional();
   }

   @Override
   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      // In transactional cache it is possible that the node is first committed as a backup, overwriting both DC and
      // cache store, and then we find out that the primary owner was lost and we have become the new primary.
      // Then we have to fire the listeners and we need the previous value.
      if (!transactional) {
         // In non-transactional cache even backups have to store the previous value in context as it may be required
         // when this node becomes primary owner for the listener invocation. Note that commands have to fire events
         // even when they know that the modification was already applied because clustered listeners and continuous
         // query require the event from primary owner.
         if (cmd.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
            return false;
         }
         // TODO [Dan] I'm not sure using the write CH is OK here
         DistributionInfo info = distributionManager.getCacheTopology().getDistribution(key);
         if (!info.isPrimary() && (!info.isWriteOwner() || ctx.isOriginLocal())) {
            if (trace) {
               log.tracef("Skip load for command %s. This node is neither the primary owner nor non-origin backup of %s", cmd, toStr(key));
            }
            return true;
         }
      }
      return false;
   }

   @Override
   protected boolean canLoad(Object key) {
      // Don't load the value if we are using distributed mode and aren't in the read CH
      return stateTransferManager.isJoinComplete() && isKeyLocal(key);
   }

   private boolean isKeyLocal(Object key) {
      return distributionManager.getCacheTopology().isReadOwner(key);
   }
}
