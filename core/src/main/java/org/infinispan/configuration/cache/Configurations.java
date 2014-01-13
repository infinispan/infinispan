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
   private Configurations() {
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

   public static boolean noDataLossOnJoiner(Configuration configuration) {
      //local caches does not have joiners
      if (!configuration.clustering().cacheMode().isClustered()) {
         return true;
      }
      //shared cache store has all the data
      if (hasSharedCacheLoaderOrWriter(configuration)) {
         return true;
      }
      final boolean usingStores = configuration.persistence().usingStores();
      final boolean passivation = configuration.persistence().passivation();
      final boolean fetchInMemoryState = configuration.clustering().stateTransfer().fetchInMemoryState();
      final boolean fetchPersistenceState = configuration.persistence().fetchPersistentState();
      //local cache store without passivation, with fetchPersistentState, regardless of fetchInMemoryState
      return (usingStores && !passivation && (fetchInMemoryState || fetchPersistenceState)) ||
            //local cache store with passivation, with fetchPersistentState && fetchInMemoryState
            (usingStores && passivation && fetchInMemoryState && fetchPersistenceState) ||
            //no cache stores, fetch in memory state
            (!usingStores && fetchInMemoryState);
   }

   public static boolean hasSharedCacheLoaderOrWriter(Configuration configuration) {
      for (StoreConfiguration storeConfiguration : configuration.persistence().stores()) {
         if (storeConfiguration.shared()) {
            return true;
         }
      }
      return false;
   }
}
