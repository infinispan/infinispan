package org.infinispan.config;

import org.infinispan.config.Configuration.EvictionType;
import org.infinispan.config.GlobalConfiguration.TransportType;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;

/**
 * ConfigurationValidatingVisitor checks semantic validity of InfinispanConfiguration instance.
 *
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 * @deprecated Belongs to old configuration and so will dissapear in future releases
 */
@SuppressWarnings("boxing")
@Deprecated
public class ConfigurationValidatingVisitor extends AbstractConfigurationBeanVisitor {

   private static final Log log = LogFactory.getLog(ConfigurationValidatingVisitor.class);

   private TransportType tt = null;
   private boolean evictionEnabled = false;
   private Configuration cfg;

   @Override
   public void visitSingletonStoreConfig(SingletonStoreConfig ssc) {
      if (tt == null && ssc.isSingletonStoreEnabled()) throw new ConfigurationException("Singleton store configured without transport being configured");
   }

   @Override
   public void visitTransportType(TransportType tt) {
      this.tt = tt;
   }

   @Override
   public void visitConfiguration(Configuration cfg) {
      this.cfg= cfg;
   }

   @Override
   public void visitClusteringType(Configuration.ClusteringType clusteringType) {
      Configuration.CacheMode mode = clusteringType.mode;
      Configuration.AsyncType async = clusteringType.async;
      Configuration.StateRetrievalType state = clusteringType.stateRetrieval;
      // certain combinations are illegal, such as state transfer + invalidation
      if (mode.isInvalidation() && state.fetchInMemoryState)
         throw new ConfigurationException("Cache cannot use INVALIDATION mode and have fetchInMemoryState set to true.");

      if (mode.isDistributed() && async.useReplQueue)
         throw new ConfigurationException("Use of the replication queue is invalid when using DISTRIBUTED mode.");

      if (mode.isSynchronous() && async.useReplQueue)
         throw new ConfigurationException("Use of the replication queue is only allowed with an ASYNCHRONOUS cluster mode.");

      // If replicated and fetch state transfer was not explicitly
      // disabled, then force enabling of state transfer
      Set<String> overriden = clusteringType.stateRetrieval.overriddenConfigurationElements;
      if (mode.isReplicated() && !state.isFetchInMemoryState()
            && !overriden.contains("fetchInMemoryState")) {
         log.debug("Cache is replicated but state transfer was not defined, so force enabling it");
         state.fetchInMemoryState(true);
      }
   }

   @Override
   public void visitL1Type(Configuration.L1Type l1Type) {
      boolean l1Enabled = l1Type.enabled;
      boolean l1OnRehash = l1Type.onRehash;

      // If L1 is disabled, L1ForRehash should also be disabled
      if (!l1Enabled && l1OnRehash) {
         Set<String> overridden = l1Type.overriddenConfigurationElements;
         if (overridden.contains("onRehash")) {
            throw new ConfigurationException("Can only move entries to L1 on rehash when L1 is enabled");
         } else {
            log.debug("L1 is disabled and L1OnRehash was not defined, disabling it");
            l1Type.onRehash(false);
         }
      }
   }

   @Override
   public void visitCacheLoaderManagerConfig(CacheLoaderManagerConfig cacheLoaderManagerConfig) {
      if (!evictionEnabled && cacheLoaderManagerConfig.isPassivation())
         log.passivationWithoutEviction();

      boolean shared = cacheLoaderManagerConfig.isShared();
      if (!shared) {
         for (CacheLoaderConfig loaderConfig : cacheLoaderManagerConfig.getCacheLoaderConfigs()) {
            if (loaderConfig instanceof CacheStoreConfig) {
               CacheStoreConfig storeConfig = (CacheStoreConfig)loaderConfig;
               Boolean fetchPersistentState = storeConfig.isFetchPersistentState();
               Boolean purgeOnStartup = storeConfig.isPurgeOnStartup();
               if (!fetchPersistentState && !purgeOnStartup && cfg.getCacheMode().isClustered()) {
                  log.staleEntriesWithoutFetchPersistentStateOrPurgeOnStartup();
               }
            }
         }
      }
   }

   @Override
   public void visitVersioningConfigurationBean(Configuration.VersioningConfigurationBean config) {
   }

   @Override
   public void visitEvictionType(EvictionType et) {
      evictionEnabled = et.strategy.isEnabled();
      if (et.strategy.isEnabled() && et.maxEntries <= 0)
         throw new ConfigurationException("Eviction maxEntries value cannot be less than or equal to zero if eviction is enabled");
   }

   @Override
   public void visitQueryConfigurationBean(Configuration.QueryConfigurationBean qcb) {
      if ( ! qcb.enabled ) {
         return;
      }
      // Check that the query module is on the classpath.
      try {
         String clazz = "org.infinispan.query.Search";
         ClassLoader classLoader;
         if ((classLoader = cfg.getClassLoader()) == null)
            Class.forName(clazz);
         else
            classLoader.loadClass(clazz);
      } catch (ClassNotFoundException e) {
         log.warnf("Indexing can only be enabled if infinispan-query.jar is available on your classpath, and this jar has not been detected. Intended behavior may not be exhibited.");
      }
   }

   @Override
   public void visitTransactionType(Configuration.TransactionType bean) {
      if (bean.transactionManagerLookup == null && bean.transactionManagerLookupClass == null) {
         if (bean.build().isInvocationBatchingEnabled()) {
            bean.transactionManagerLookupClass = null;
            if (!bean.useSynchronization) log.debug("Switching to Synchronization-based enlistment.");
            bean.useSynchronization = true;
         }
      }
   }

}
