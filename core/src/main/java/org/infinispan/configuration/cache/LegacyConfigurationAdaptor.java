package org.infinispan.configuration.cache;

import org.infinispan.config.Configuration;
import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.config.FluentConfiguration;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.AbstractCacheStoreConfig;

public class LegacyConfigurationAdaptor {

   public org.infinispan.config.Configuration adapt(org.infinispan.configuration.cache.Configuration config) {
      
      // Handle the case that null is passed in
      if (config == null)
         return null;
      
      FluentConfiguration legacy = new Configuration().fluent();
      
      legacy.clustering()
         .mode(CacheMode.valueOf(config.clustering().cacheMode().name()));
      
      if (!config.clustering().cacheMode().isSynchronous()) {
         legacy.clustering()
            .async()
               .asyncMarshalling(config.clustering().async().asyncMarshalling())
               .replQueueClass(config.clustering().async().replQueue().getClass())
               .replQueueInterval(config.clustering().async().replQueueInterval())
               .replQueueMaxElements(config.clustering().async().replQueueMaxElements());
      }
      
      if (config.clustering().hash().consistentHash() != null) {
         legacy.clustering()
            .hash()
               .consistentHashClass(config.clustering().hash().consistentHash().getClass());
      
      }
      if (config.clustering().hash().hash() != null) {
         legacy.clustering()
            .hash()
               .hashFunctionClass(config.clustering().hash().hash().getClass());
      }
      legacy.clustering()
      .hash()
            .numOwners(config.clustering().hash().numOwners())
            .numVirtualNodes(config.clustering().hash().numVirtualNodes())
            .rehashEnabled(config.clustering().hash().isRehashEnabled())
            .rehashRpcTimeout(config.clustering().hash().rehashRpcTimeout())
            .rehashWait(config.clustering().hash().rehashWait())
            .groups()
               .enabled(config.clustering().hash().groupsConfiguration().enabled())
               .groupers(config.clustering().hash().groupsConfiguration().groupers());
      
      if (config.clustering().l1().isEnabled()) {
         legacy.clustering()
            .l1()
               .invalidationThreshold(config.clustering().l1().invalidationThreshold())
               .lifespan(config.clustering().l1().lifespan())
               .onRehash(config.clustering().l1().onRehash());
      } else {
         legacy.clustering()
            .l1()
               .disable();
      }
      
      legacy.clustering()
         .stateRetrieval()
            .alwaysProvideInMemoryState(config.clustering().stateRetrieval().alwaysProvideInMemoryState())
            .fetchInMemoryState(config.clustering().stateRetrieval().fetchInMemoryState())
            .initialRetryWaitTime(config.clustering().stateRetrieval().initialRetryWaitTime())
            .logFlushTimeout(config.clustering().stateRetrieval().logFlushTimeout())
            .maxNonProgressingLogWrites(config.clustering().stateRetrieval().maxNonPorgressingLogWrites())
            .numRetries(config.clustering().stateRetrieval().numRetries())
            .retryWaitTimeIncreaseFactor(config.clustering().stateRetrieval().retryWaitTimeIncreaseFactor())
            .timeout(config.clustering().stateRetrieval().timeout());
      
      if (config.clustering().cacheMode().isSynchronous()) {
         legacy.clustering()
            .sync()
               .replTimeout(config.clustering().sync().replTimeout());
      }
      
      for (CommandInterceptor interceptor : config.customInterceptors().interceptors()) {
         legacy.clustering()
         .customInterceptors()
            .add(interceptor);
      }
      
      legacy.dataContainer()
         .dataContainer(config.dataContainer().dataContainer())
         .withProperties(config.dataContainer().properties());
      
      if (config.deadlockDetection().enabled()) {
         legacy.deadlockDetection()
            .spinDuration(config.deadlockDetection().spinDuration());
      } else {
         legacy.deadlockDetection()
            .disable();
      }
      
      legacy.eviction()
         .maxEntries(config.eviction().maxEntries())
         .strategy(config.eviction().strategy())
         .threadPolicy(config.eviction().threadPolicy());
      
      legacy.expiration()
         .lifespan(config.expiration().lifespan())
         .maxIdle(config.expiration().maxIdle())
         .reaperEnabled(config.expiration().reaperEnabled())
         .wakeUpInterval(config.expiration().wakeUpInterval());
         
      if (config.indexing().enabled())
         legacy.indexing()
            .indexLocalOnly(config.indexing().indexLocalOnly());
      else
         legacy.indexing()
            .disable();
         
      if (config.invocationBatching().enabled())
         legacy.invocationBatching();

      if (config.jmxStatistics().enabled())
         legacy.jmxStatistics();
      
      // TODO lazy deserialization?
      
      legacy.loaders()
         .passivation(config.loaders().passivation())
         .preload(config.loaders().preload())
         .shared(config.loaders().shared());

      for (LoaderConfiguration loader : config.loaders().cacheLoaders()) {
         AbstractCacheStoreConfig csc = new AbstractCacheStoreConfig();
         csc.setCacheLoaderClassName(loader.cacheLoader().getClass().getName());
         csc.fetchPersistentState(loader.fetchPersistentState());
         csc.ignoreModifications(loader.ignoreModifications());
         csc.purgeOnStartup(loader.purgeOnStartup());
         csc.purgerThreads(loader.purgerThreads());
         csc.setPurgeSynchronously(loader.purgeSynchronously());
         csc.getAsyncStoreConfig().setEnabled(loader.async().enabled());
         csc.getAsyncStoreConfig().flushLockTimeout(loader.async().flushLockTimeout());
         csc.getAsyncStoreConfig().modificationQueueSize(loader.async().modificationQueueSize());
         csc.getAsyncStoreConfig().shutdownTimeout(loader.async().shutdownTimeout());
         csc.getAsyncStoreConfig().threadPoolSize(loader.async().threadPoolSize());
         csc.setProperties(loader.properties());
         csc.getSingletonStoreConfig().enabled(loader.singletonStore().enabled());
         csc.getSingletonStoreConfig().pushStateTimeout(loader.singletonStore().pushStateTimeout());
         csc.getSingletonStoreConfig().pushStateWhenCoordinator(loader.singletonStore().pushStateWhenCoordinator());
         legacy.loaders().addCacheLoader(csc);
      }
      
      legacy.locking()
         .concurrencyLevel(config.locking().concurrencyLevel())
         .isolationLevel(config.locking().isolationLevel())
         .lockAcquisitionTimeout(config.locking().lockAcquisitionTimeout())
         .useLockStriping(config.locking().useLockStriping())
         .writeSkewCheck(config.locking().writeSkewCheck());
      
      if (config.storeAsBinary().enabled()) 
         legacy.storeAsBinary()
            .storeKeysAsBinary(config.storeAsBinary().storeKeysAsBinary())
            .storeValuesAsBinary(config.storeAsBinary().storeValuesAsBinary());
      else
         legacy.storeAsBinary()
            .disable();
   
      legacy.transaction()
         .autoCommit(config.transaction().autoCommit())
         .cacheStopTimeout((int) config.transaction().cacheStopTimeout())
         .eagerLockSingleNode(config.transaction().eagerLockingSingleNode())
         .lockingMode(config.transaction().lockingMode())
         .syncCommitPhase(config.transaction().syncCommitPhase())
         .syncRollbackPhase(config.transaction().syncRollbackPhase())
         .transactionManagerLookup(config.transaction().transactionManagerLookup())
         .transactionMode(config.transaction().transactionMode())
         .useEagerLocking(config.transaction().useEagerLocking())
         .useSynchronization(config.transaction().useSynchronization());
      
      if (config.transaction().recovery().enabled()) {
         legacy.transaction().recovery();
      }
        
      legacy.unsafe().unreliableReturnValues(config.unsafe().unreliableReturnValues());

      if (config.versioningConfiguration().enabled()) {
         legacy.versioning()
               .enable()
               .versioningScheme(config.versioningConfiguration().scheme());
      }
      
      return legacy.build();
   }
}