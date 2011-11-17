package org.infinispan.configuration.cache;

import org.infinispan.config.Configuration;
import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.config.FluentConfiguration;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.AbstractCacheStoreConfig;

public class LegacyConfigurationAdaptor {

   public org.infinispan.config.Configuration adapt(org.infinispan.configuration.cache.Configuration config) {
      FluentConfiguration legacy = new Configuration().fluent();
      
      legacy.clustering()
         .mode(CacheMode.valueOf(config.getClustering().getCacheMode().name()));
      
      legacy.clustering()
         .async()
            .asyncMarshalling(config.getClustering().getAsync().isAsyncMarshalling())
            .replQueueClass(config.getClustering().getAsync().getReplQueue().getClass())
            .replQueueInterval(config.getClustering().getAsync().getReplQueueInterval())
            .replQueueMaxElements(config.getClustering().getAsync().getReplQueueMaxElements());
      
      legacy.clustering()
         .hash()
            .consistentHashClass(config.getClustering().getHash().getConsistentHash().getClass())
            .hashFunctionClass(config.getClustering().getHash().getHash().getClass())
            .hashSeed(config.getClustering().getHash().getHashSeed())
            .numOwners(config.getClustering().getHash().getNumOwners())
            .numVirtualNodes(config.getClustering().getHash().getNumVirtualNodes())
            .rehashEnabled(config.getClustering().getHash().isRehashEnabled())
            .rehashRpcTimeout(config.getClustering().getHash().getRehashRpcTimeout())
            .rehashWait(config.getClustering().getHash().getRehashWait())
            .groups()
               .enabled(config.getClustering().getHash().getGroupsConfiguration().isEnabled())
               .groupers(config.getClustering().getHash().getGroupsConfiguration().getGroupers());
      
      if (config.getClustering().getL1().isEnabled()) {
         legacy.clustering()
            .l1()
               .invalidationThreshold(config.getClustering().getL1().getInvalidationThreshold())
               .lifespan(config.getClustering().getL1().getLifespan())
               .onRehash(config.getClustering().getL1().isOnRehash());
      } else {
         legacy.clustering()
            .l1()
               .disable();
      }
      
      legacy.clustering()
         .stateRetrieval()
            .alwaysProvideInMemoryState(config.getClustering().getStateRetrieval().isAlwaysProvideInMemoryState())
            .fetchInMemoryState(config.getClustering().getStateRetrieval().isFetchInMemoryState())
            .initialRetryWaitTime(config.getClustering().getStateRetrieval().getInitialRetryWaitTime())
            .logFlushTimeout(config.getClustering().getStateRetrieval().getLogFlushTimeout())
            .maxNonProgressingLogWrites(config.getClustering().getStateRetrieval().getMaxNonPorgressingLogWrites())
            .numRetries(config.getClustering().getStateRetrieval().getNumRetries())
            .retryWaitTimeIncreaseFactor(config.getClustering().getStateRetrieval().getRetryWaitTimeIncreaseFactor())
            .timeout(config.getClustering().getStateRetrieval().getTimeout());
      
      legacy.clustering()
         .sync()
            .replTimeout(config.getClustering().getSync().getReplTimeout());
      
      for (CommandInterceptor interceptor : config.getCustomInterceptors().getInterceptors()) {
         legacy.clustering()
         .customInterceptors()
            .add(interceptor);
      }
      
      legacy.dataContainer()
         .dataContainer(config.getDataContainer().getDataContainer())
         .withProperties(config.getDataContainer().getProperties());
      
      if (config.getDeadlockDetection().isEnabled()) {
         legacy.deadlockDetection()
            .spinDuration(config.getDeadlockDetection().getSpinDuration());
      } else {
         legacy.deadlockDetection()
            .disable();
      }
      
      legacy.eviction()
         .maxEntries(config.getEviction().getMaxEntries())
         .strategy(config.getEviction().getStrategy())
         .threadPolicy(config.getEviction().getThreadPolicy());
      
      legacy.expiration()
         .lifespan(config.getExpiration().getLifespan())
         .maxIdle(config.getExpiration().getMaxIdle())
         .reaperEnabled(config.getExpiration().isReaperEnabled())
         .wakeUpInterval(config.getExpiration().getWakeUpInterval());
         
      if (config.getIndexing().isEnabled())
         legacy.indexing()
            .indexLocalOnly(config.getIndexing().isIndexLocalOnly());
      else
         legacy.indexing()
            .disable();
         
      if (config.getInvocationBatching().isEnabled())
         legacy.invocationBatching();

      if (config.getJmxStatistics().isEnabled())
         legacy.jmxStatistics();
      
      // TODO lazy deserialization?
      
      legacy.loaders()
         .passivation(config.getLoaders().isPassivation())
         .preload(config.getLoaders().isPreload())
         .shared(config.getLoaders().isShared());

      for (LoaderConfiguration loader : config.getLoaders().getCacheLoaders()) {
         AbstractCacheStoreConfig csc = new AbstractCacheStoreConfig();
         csc.setCacheLoaderClassName(loader.getCacheLoader().getClass().getName());
         csc.fetchPersistentState(loader.isFetchPersistentState());
         csc.ignoreModifications(loader.isIgnoreModifications());
         csc.purgeOnStartup(loader.isPurgeOnStartup());
         csc.purgerThreads(loader.getPurgerThreads());
         csc.setPurgeSynchronously(loader.isPurgeSynchronously());
         csc.getAsyncStoreConfig().setEnabled(loader.getAsync().isEnabled());
         csc.getAsyncStoreConfig().flushLockTimeout(loader.getAsync().getFlushLockTimeout());
         csc.getAsyncStoreConfig().modificationQueueSize(loader.getAsync().getModificationQueueSize());
         csc.getAsyncStoreConfig().shutdownTimeout(loader.getAsync().getShutdownTimeout());
         csc.getAsyncStoreConfig().threadPoolSize(loader.getAsync().getThreadPoolSize());
         csc.setProperties(loader.getProperties());
         csc.getSingletonStoreConfig().enabled(loader.getSingletonStore().isEnabled());
         csc.getSingletonStoreConfig().pushStateTimeout(loader.getSingletonStore().getPushStateTimeout());
         csc.getSingletonStoreConfig().pushStateWhenCoordinator(loader.getSingletonStore().isPushStateWhenCoordinator());
         legacy.loaders().addCacheLoader(csc);
      }
      
      legacy.locking()
         .concurrencyLevel(config.getLocking().getConcurrencyLevel())
         .isolationLevel(config.getLocking().getIsolationLevel())
         .lockAcquisitionTimeout(config.getLocking().getLockAcquisitionTimeout())
         .useLockStriping(config.getLocking().isUseLockStriping())
         .writeSkewCheck(config.getLocking().isWriteSkewCheck());
      
      if (config.getStoreAsBinary().isEnabled()) 
         legacy.storeAsBinary()
            .storeKeysAsBinary(config.getStoreAsBinary().isStoreKeysAsBinary())
            .storeValuesAsBinary(config.getStoreAsBinary().isStoreValuesAsBinary());
      else
         legacy.storeAsBinary()
            .disable();
   
      legacy.transaction()
         .autoCommit(config.getTransaction().isAutoCommit())
         .cacheStopTimeout(config.getTransaction().getCacheStopTimeout())
         .eagerLockSingleNode(config.getTransaction().isEagerLockingSingleNode())
         .lockingMode(config.getTransaction().getLockingMode())
         .syncCommitPhase(config.getTransaction().isSyncCommitPhase())
         .syncRollbackPhase(config.getTransaction().isSyncRollbackPhase())
         .transactionManagerLookup(config.getTransaction().getTransactionManagerLookup())
         .transactionMode(config.getTransaction().getTransactionMode())
         .useEagerLocking(config.getTransaction().isUseEagerLocking())
         .useSynchronization(config.getTransaction().isUseSynchronization());
      
      if (config.getTransaction().getRecovery().isEnabled()) {
         legacy.transaction().recovery();
      }
        
      legacy.unsafe().unreliableReturnValues(config.getUnsafe().isUnreliableReturnValues());
      
      
      return legacy.build();
   }
}