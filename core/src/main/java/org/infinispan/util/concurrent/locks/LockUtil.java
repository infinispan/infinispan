package org.infinispan.util.concurrent.locks;

import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.distribution.Ownership;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

/**
 * Utility methods for locking keys.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class LockUtil {

   private LockUtil() {
   }

   /**
    * It filters the {@code key} by lock ownership.
    *
    * @param key                      the key to check
    * @param clusteringDependentLogic the {@link ClusteringDependentLogic} to check the ownership of the keys.
    * @return the {@link Ownership}.
    * @throws NullPointerException if {@code clusteringDependentLogic} is {@code null}.
    */
   public static Ownership getLockOwnership(Object key, ClusteringDependentLogic clusteringDependentLogic) {
      Object keyToCheck = key instanceof DeltaCompositeKey ?
            ((DeltaCompositeKey) key).getDeltaAwareValueKey() :
            key;
      if (clusteringDependentLogic.localNodeIsPrimaryOwner(keyToCheck)) {
         return Ownership.PRIMARY;
      } else if (clusteringDependentLogic.localNodeIsOwner(keyToCheck)) {
         return Ownership.BACKUP;
      } else {
         return Ownership.NON_OWNER;
      }
   }


}
