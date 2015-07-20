package org.infinispan.util.concurrent.locks;

import org.infinispan.atomic.DeltaCompositeKey;
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
    * @return the {@link org.infinispan.util.concurrent.locks.LockUtil.LockOwnership}.
    * @throws NullPointerException if {@code clusteringDependentLogic} is {@code null}.
    */
   public static LockOwnership getLockOwnership(Object key, ClusteringDependentLogic clusteringDependentLogic) {
      Object keyToCheck = key instanceof DeltaCompositeKey ?
            ((DeltaCompositeKey) key).getDeltaAwareValueKey() :
            key;
      if (clusteringDependentLogic.localNodeIsPrimaryOwner(keyToCheck)) {
         return LockOwnership.PRIMARY;
      } else if (clusteringDependentLogic.localNodeIsOwner(keyToCheck)) {
         return LockOwnership.BACKUP;
      } else {
         return LockOwnership.NO_OWNER;
      }
   }


   public enum LockOwnership {
      /**
       * This node is not an owner.
       */
      NO_OWNER,
      /**
       * This node is the primary lock owner.
       */
      PRIMARY,
      /**
       * this node is the backup lock owner.
       */
      BACKUP
   }

}
