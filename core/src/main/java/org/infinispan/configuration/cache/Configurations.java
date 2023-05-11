package org.infinispan.configuration.cache;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.internal.PrivateGlobalConfiguration;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;

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

   public static boolean isExceptionBasedEviction(Configuration cfg) {
      return cfg.memory().size() > 0 && cfg.memory().evictionStrategy().isExceptionBased();
   }

   public static boolean isOnePhaseCommit(Configuration cfg) {
      // Otherwise pessimistic transactions will be one phase commit
      if (isExceptionBasedEviction(cfg)) {
         return false;
      }
      return !cfg.clustering().cacheMode().isSynchronous() ||
            cfg.transaction().lockingMode() == LockingMode.PESSIMISTIC;
   }

   public static boolean isTxVersioned(Configuration cfg) {
      return cfg.transaction().transactionMode().isTransactional() &&
            cfg.transaction().lockingMode() == LockingMode.OPTIMISTIC &&
            cfg.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ &&
            !cfg.clustering().cacheMode().isInvalidation(); //invalidation can't use versions
   }

   public static boolean isEmbeddedMode(GlobalConfiguration globalConfiguration) {
      PrivateGlobalConfiguration config = globalConfiguration.module(PrivateGlobalConfiguration.class);
      return config == null || !config.isServerMode();
   }

   public static boolean isClustered(GlobalConfiguration globalConfiguration) {
      return globalConfiguration.transport().transport() != null;
   }

   /**
    * Returns if the store configuration is a store that is used for state transfer. This is no longer used and will
    * return true if the store config is not shared.
    *
    * @param storeConfiguration Store configuration to check
    * @return if the store config can be used for state transfer
    * @deprecated since 14.0. Returns true if the store is not shared.
    */
   @Deprecated
   public static boolean isStateTransferStore(StoreConfiguration storeConfiguration) {
      return !storeConfiguration.shared();
   }

   public static boolean needSegments(Configuration configuration) {
      CacheMode cacheMode = configuration.clustering().cacheMode();
      boolean transactional = configuration.transaction().transactionMode().isTransactional();
      boolean usingSegmentedStore = configuration.persistence().usingSegmentedStore();
      return (cacheMode.isReplicated() ||
            cacheMode.isDistributed() ||
            (cacheMode.isInvalidation() && transactional) ||
            usingSegmentedStore);
   }

   public static Metadata newDefaultMetadata(Configuration configuration) {
      return new EmbeddedMetadata.Builder()
            .lifespan(configuration.expiration().lifespan())
            .maxIdle(configuration.expiration().maxIdle())
            .build();
   }
}
