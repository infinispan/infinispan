package org.infinispan.configuration.cache;

import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;

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

   /**
    * Strict optimistic transactions require repeteable read, write skew and
    * simple versioning configuration in order for conditional cache
    * operations to behave as expected. This method returns true if the
    * configuration is strictly optimistic transactional.
    *
    * @param builder configuration to inspect to make strict optimistic
    *                transaction decision.
    * @return true if configuration is strictly optimistic transactional,
    * false otherwise.
    */
   public static boolean isStrictOptimisticTransaction(ConfigurationBuilder builder) {
      TransactionConfigurationBuilder transactionBuilder = builder.transaction();
      TransactionMode txMode = transactionBuilder.transactionMode;
      CacheMode cacheMode = builder.clustering().cacheMode();
      return txMode != null
            && txMode.isTransactional()
            && transactionBuilder.lockingMode == LockingMode.OPTIMISTIC
            && cacheMode.isSynchronous()
            && !cacheMode.isInvalidation();
   }

}
