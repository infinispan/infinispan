package org.infinispan.interceptors;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

import java.util.Set;

/**
 * The same as a regular cache loader interceptor, except that it contains additional logic to force loading from the
 * cache loader if needed on a remote node, in certain conditions.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ClusteredCacheLoaderInterceptor extends CacheLoaderInterceptor {

   private boolean isWriteSkewConfigured;
   private ClusteringDependentLogic cdl;
   private DistributionManager dm;

   @Inject
   private void injectDependencies(ClusteringDependentLogic cdl, DistributionManager dm) {
      this.cdl = cdl;
      this.dm = dm;
   }
   
   @Start(priority = 15)
   private void startClusteredCacheLoaderInterceptor() {
      CacheMode cacheMode = cacheConfiguration.clustering().cacheMode();
      // For now the primary data owner may need to load from the cache store, even if
      // this is a remote call, if write skew checking is enabled.  Once ISPN-317 is in, this may also need to
      // happen if running in distributed mode and eviction is enabled.
      isWriteSkewConfigured = cacheConfiguration.locking().writeSkewCheck()
            && (cacheMode.isReplicated() || cacheMode.isDistributed());
   }

   @Override
   protected boolean forceLoad(Object key, Set<Flag> flags) {
      return isDeltaWrite(flags) || isWriteSkewConfigured && cdl.localNodeIsPrimaryOwner(key);
   }

   @Override
   protected boolean canLoad(Object key) {
      // Don't load the value if we are using distributed mode and aren't in the read CH
      return !cacheConfiguration.clustering().cacheMode().isDistributed() ||
            dm.getReadConsistentHash().isKeyLocalToNode(cdl.getAddress(), key);
   }
}
