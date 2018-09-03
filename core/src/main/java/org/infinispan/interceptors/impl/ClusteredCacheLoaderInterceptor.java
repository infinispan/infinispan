package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
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

   @Inject private DistributionManager distributionManager;

   private boolean transactional;

   @Start(priority = 15)
   private void startClusteredCacheLoaderInterceptor() {
      transactional = cacheConfiguration.transaction().transactionMode().isTransactional();
   }

   @Override
   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      if (transactional) {
         // LoadType.OWNER is used when the previous value is required to produce new value itself (functional commands
         // or delta-aware), therefore, we have to load them into context. Other load types have checked the value
         // already on the originator and therefore the value is loaded only for WSC (without this interceptor)
         if (!ctx.isOriginLocal() && cmd.loadType() != VisitableCommand.LoadType.OWNER) {
            return true;
         }
      } else {
         switch (cmd.loadType()) {
            case DONT_LOAD:
               return true;
            case PRIMARY:
               if (cmd.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
                  return cmd.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD);
               }
               if (!distributionManager.getCacheTopology().getDistribution(key).isPrimary()) {
                  if (trace) {
                     log.tracef("Skip load for command %s. This node is not the primary owner of %s", cmd, toStr(key));
                  }
                  return true;
               }
               break;
            case OWNER:
               if (cmd.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
                  return cmd.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD);
               }
               // TODO [Dan] I'm not sure using the write CH is OK here
               DistributionInfo info = distributionManager.getCacheTopology().getDistribution(key);
               if (!info.isPrimary() && (!info.isWriteOwner() || ctx.isOriginLocal())) {
                  if (trace) {
                     log.tracef("Skip load for command %s. This node is neither the primary owner nor non-origin backup of %s", cmd, toStr(key));
                  }
                  return true;
               }
               break;
         }
      }
      return super.skipLoadForWriteCommand(cmd, key, ctx);
   }

   @Override
   protected boolean canLoad(Object key) {
      // Don't load the value if we are using distributed mode and aren't in the read CH
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      return cacheTopology != null && cacheTopology.isReadOwner(key);
   }

}
