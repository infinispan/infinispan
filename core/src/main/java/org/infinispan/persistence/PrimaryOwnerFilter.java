package org.infinispan.persistence;

import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.spi.AdvancedCacheLoader;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class PrimaryOwnerFilter implements AdvancedCacheLoader.KeyFilter {

   private final ClusteringDependentLogic cdl;

   public PrimaryOwnerFilter(ClusteringDependentLogic cdl) {
      this.cdl = cdl;
   }

   @Override
   public boolean shouldLoadKey(Object key) {
      return cdl.localNodeIsPrimaryOwner(key);
   }
}
