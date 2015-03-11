package org.infinispan.persistence;

import org.infinispan.distribution.LookupMode;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class PrimaryOwnerFilter<K> implements KeyFilter<K> {

   private final ClusteringDependentLogic cdl;

   public PrimaryOwnerFilter(ClusteringDependentLogic cdl) {
      this.cdl = cdl;
   }

   @Override
   public boolean accept(K key) {
      return cdl.localNodeIsPrimaryOwner(key, LookupMode.READ);
   }
}
