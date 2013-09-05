package org.infinispan.configuration.cache;

import org.infinispan.transaction.LockingMode;

/**
 * Helper configuration methods.
 *
 * @author Galder Zamarreño
 * @author Pedro Ruivo
 * @since 5.2
 */
public class Configurations {

   // Suppresses default constructor, ensuring non-instantiability.
   private Configurations(){
   }

   public static boolean isSecondPhaseAsync(Configuration cfg) {
      ClusteringConfiguration clusteringCfg = cfg.clustering();
      return !cfg.transaction().syncCommitPhase()
            || clusteringCfg.async().useReplQueue()
            || !clusteringCfg.cacheMode().isSynchronous();
   }

   public static boolean isOnePhaseCommit(Configuration cfg) {
      return !cfg.clustering().cacheMode().isSynchronous() ||
            cfg.transaction().lockingMode() == LockingMode.PESSIMISTIC;
   }

   public static boolean isOnePhaseTotalOrderCommit(Configuration cfg) {
      return cfg.transaction().transactionProtocol().isTotalOrder() && !isVersioningEnabled(cfg);
   }

   public static boolean isVersioningEnabled(Configuration cfg) {
      return cfg.locking().writeSkewCheck() &&
            cfg.transaction().lockingMode() == LockingMode.OPTIMISTIC &&
            cfg.versioning().enabled();
   }

}
