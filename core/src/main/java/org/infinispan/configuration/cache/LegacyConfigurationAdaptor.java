/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

import java.util.Properties;

import org.infinispan.commons.hash.Hash;
import org.infinispan.config.Configuration;
import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.config.CustomInterceptorConfig;
import org.infinispan.config.FluentConfiguration;
import org.infinispan.config.FluentConfiguration.CustomInterceptorPosition;
import org.infinispan.config.FluentConfiguration.IndexingConfig;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.AbstractCacheLoaderConfig;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.Util;

@SuppressWarnings({"deprecation", "boxing"})
public class LegacyConfigurationAdaptor {
   private LegacyConfigurationAdaptor() {
      // Hide constructor
   }

   public static org.infinispan.config.Configuration adapt(org.infinispan.configuration.cache.Configuration config) {

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
               .replQueueMaxElements(config.clustering().async().replQueueMaxElements())
               .useReplQueue(config.clustering().async().useReplQueue());
      }

      if (config.clustering().hash().hash() != null) {
         legacy.clustering()
            .hash()
               .hashFunctionClass(config.clustering().hash().hash().getClass());
      }

      legacy.clustering()
         .hash()
            .numOwners(config.clustering().hash().numOwners())
            .groups()
               .enabled(config.clustering().hash().groups().enabled())
               .groupers(config.clustering().hash().groups().groupers());

      if (config.clustering().cacheMode().isDistributed()) {
         legacy.clustering()
               .hash()
               .rehashEnabled(config.clustering().stateTransfer().fetchInMemoryState())
               .rehashRpcTimeout(config.clustering().stateTransfer().timeout())
               .rehashWait(config.clustering().stateTransfer().timeout());
      } else if (config.clustering().cacheMode().isClustered()) { // REPL or DIST
         legacy.clustering()
               .stateRetrieval()
               .fetchInMemoryState(config.clustering().stateTransfer().fetchInMemoryState())
               .timeout(config.clustering().stateTransfer().timeout());
      }
      if (config.clustering().l1().enabled()) {
         legacy.clustering()
            .l1()
               .invalidationThreshold(config.clustering().l1().invalidationThreshold())
               .lifespan(config.clustering().l1().lifespan())
               .onRehash(config.clustering().l1().onRehash())
               .cleanupTaskFrequency(config.clustering().l1().cleanupTaskFrequency());
      } else {
         legacy.clustering()
            .l1()
               .disable()
               .onRehash(config.clustering().l1().onRehash());
      }

      // We have only defined the chunkSize in the legacy stateRetrieval config, but we are using it in distributed mode as well
      legacy.clustering()
         .stateRetrieval()
            .chunkSize(config.clustering().stateTransfer().chunkSize());

      if (config.clustering().cacheMode().isSynchronous()) {
         legacy.clustering()
            .sync()
               .replTimeout(config.clustering().sync().replTimeout());
      }

      for (InterceptorConfiguration interceptor : config.customInterceptors().interceptors()) {
         CustomInterceptorPosition position = legacy.customInterceptors()
            .add(interceptor.interceptor());
         if (interceptor.after() != null)
            position.after(interceptor.after());
         if (interceptor.index() > -1)
         position.atIndex(interceptor.index());
         if (interceptor.before() != null)
            position.before(interceptor.before());
         if (interceptor.first())
            position.first();
         if (interceptor.last())
            position.last();
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

      if (config.indexing().enabled()) {
         IndexingConfig indexing = legacy.indexing();
         indexing.indexLocalOnly(config.indexing().indexLocalOnly());
         indexing.withProperties(config.indexing().properties());
      }
      else
         legacy.indexing()
            .disable();

      if (config.invocationBatching().enabled())
         legacy.invocationBatching();
      else
         legacy.invocationBatching().disable();

      if (config.jmxStatistics().enabled())
         legacy.jmxStatistics();

      // TODO lazy deserialization?

      legacy.loaders()
         .passivation(config.loaders().passivation())
         .preload(config.loaders().preload())
         .shared(config.loaders().shared());

      for (LoaderConfiguration loader : config.loaders().cacheLoaders()) {
         CacheLoaderConfig clc = adapt(loader);
         legacy.loaders().addCacheLoader(clc);
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
         .transactionSynchronizationRegistryLookup(config.transaction().transactionSynchronizationRegistryLookup())
         .useEagerLocking(config.transaction().useEagerLocking())
         .useSynchronization(config.transaction().useSynchronization())
         .use1PcForAutoCommitTransactions(config.transaction().use1PcForAutoCommitTransactions());

      if (config.transaction().recovery().enabled()) {
         legacy.transaction().recovery().recoveryInfoCacheName(config.transaction().recovery().recoveryInfoCacheName());
      }

      legacy.unsafe().unreliableReturnValues(config.unsafe().unreliableReturnValues());

      if (config.versioning().enabled()) {
         legacy.versioning()
               .enable()
               .versioningScheme(config.versioning().scheme());
      }

      return legacy.build();
   }

   public static CacheLoaderConfig adapt(LoaderConfiguration loader) {
      CacheLoaderConfig clc = null;
      if (loader instanceof LegacyLoaderAdapter<?>) {
         return ((LegacyLoaderAdapter<?>)loader).adapt();
      } else if (loader instanceof StoreConfiguration) {
         if (loader instanceof LegacyStoreConfiguration) {
            LegacyStoreConfiguration store = (LegacyStoreConfiguration) loader;
            CacheLoader cacheStore = store.cacheStore(); // TODO: in 6.0, as we deprecate the LegacyConfigurationLoader#cacheLoader() method, narrow this type to CacheStore
            clc = getLoaderConfig(loader, cacheStore);
         }
         CacheStoreConfig csc = (CacheStoreConfig) clc;
         StoreConfiguration store = (StoreConfiguration) loader;
         csc.fetchPersistentState(store.fetchPersistentState());
         csc.ignoreModifications(store.ignoreModifications());
         csc.purgeOnStartup(store.purgeOnStartup());
         csc.setPurgeSynchronously(store.purgeSynchronously());
         csc.getAsyncStoreConfig().setEnabled(store.async().enabled());
         csc.getAsyncStoreConfig().flushLockTimeout(store.async().flushLockTimeout());
         csc.getAsyncStoreConfig().modificationQueueSize(store.async().modificationQueueSize());
         csc.getAsyncStoreConfig().shutdownTimeout(store.async().shutdownTimeout());
         csc.getAsyncStoreConfig().threadPoolSize(store.async().threadPoolSize());
         csc.getSingletonStoreConfig().enabled(store.singletonStore().enabled());
         csc.getSingletonStoreConfig().pushStateTimeout(store.singletonStore().pushStateTimeout());
         csc.getSingletonStoreConfig().pushStateWhenCoordinator(store.singletonStore().pushStateWhenCoordinator());
      } else if (loader instanceof LegacyLoaderConfiguration) {
         CacheLoader cacheLoader = ((LegacyLoaderConfiguration) loader).cacheLoader();
         clc = getLoaderConfig(loader, cacheLoader);
      }
      if (clc instanceof AbstractCacheStoreConfig) {
         AbstractCacheStoreConfig acsc = (AbstractCacheStoreConfig) clc;
         Properties p = loader.properties();
         acsc.setProperties(p);
         if (p != null) XmlConfigHelper.setValues(clc, p, false, true);
         if (loader instanceof LegacyStoreConfiguration)
            acsc.purgerThreads(((LegacyStoreConfiguration)loader).purgerThreads());
      } else if (clc instanceof CacheLoaderConfig) {
         Properties p = loader.properties();
         if (p != null) XmlConfigHelper.setValues(clc, p, false, true);
      }
      return clc;
   }

   private static CacheLoaderConfig getLoaderConfig(LoaderConfiguration loader, CacheLoader cacheLoader) {
      if (cacheLoader.getClass().isAnnotationPresent(CacheLoaderMetadata.class)) {
         return Util.getInstance(cacheLoader.getClass().getAnnotation(CacheLoaderMetadata.class).configurationClass());
      } else {
         AbstractCacheStoreConfig acsc = new AbstractCacheStoreConfig();
         if (loader instanceof LegacyStoreConfiguration) {
            acsc.setCacheLoaderClassName(((LegacyStoreConfiguration) loader).cacheStore().getClass().getName());
         } else {
            acsc.setCacheLoaderClassName(((LegacyLoaderConfiguration) loader).cacheLoader().getClass().getName());
         }
         return acsc;
      }
   }

   public static org.infinispan.configuration.cache.Configuration adapt(org.infinispan.config.Configuration legacy) {

      // Handle the case that null is passed in
      if (legacy == null)
         return null;

      ConfigurationBuilder builder = new ConfigurationBuilder();

      builder.clustering()
         .cacheMode(org.infinispan.configuration.cache.CacheMode.valueOf(legacy.getCacheMode().name()));

      if (!legacy.getCacheMode().isSynchronous()) {
         if (legacy.isUseAsyncMarshalling())
            builder.clustering()
               .async()
                  .asyncMarshalling();
         else
            builder.clustering()
            .async()
               .syncMarshalling();

         builder.clustering()
            .async()
               .replQueue(Util.<ReplicationQueue>getInstance(legacy.getReplQueueClass(), legacy.getClassLoader()))
               .replQueueInterval(legacy.getReplQueueInterval())
               .replQueueMaxElements(legacy.getReplQueueMaxElements())
               .useReplQueue(legacy.isUseReplQueue());
      }

      if (legacy.isCustomConsistentHashClass()) {
         // We don't support custom consistent hash via hash.consistentHash any more, so this code is no longer called
         builder.clustering()
            .hash()
               .consistentHash(Util.<ConsistentHash>getInstance(legacy.getConsistentHashClass(), legacy.getClassLoader()));
      }

      // Order is important here. First check whether state transfer itself
      // has been enabled and then check whether rehashing has been enabled,
      // which both activate the same flag.
      builder.clustering()
            .stateTransfer()
            .fetchInMemoryState(legacy.isFetchInMemoryState())
            .timeout(legacy.getStateRetrievalTimeout())
            .chunkSize(legacy.getStateRetrievalChunkSize());

      if (legacy.isHashActivated()) {
         builder.clustering()
            .hash()
               .numOwners(legacy.getNumOwners())
               .rehashEnabled(legacy.isRehashEnabled())
               .rehashRpcTimeout(legacy.getRehashRpcTimeout())
               .rehashWait(legacy.getRehashWaitTime())
               .groups()
                  .enabled(legacy.isGroupsEnabled())
                  .withGroupers(legacy.getGroupers());
      }

      if (legacy.isL1CacheEnabled() && legacy.getCacheMode().isDistributed()) {
         builder.clustering().l1().enable();
         builder.clustering().l1().onRehash(legacy.isL1OnRehash());
      }
      else {
         builder.clustering().l1().disable();
      }

      builder.clustering().l1()
            .invalidationThreshold(legacy.getL1InvalidationThreshold())
            .lifespan(legacy.getL1Lifespan())
            .cleanupTaskFrequency(legacy.getL1InvalidationCleanupTaskFrequency());

      if (legacy.getCacheMode().isDistributed()) {
         builder.clustering()
               .stateTransfer()
               .fetchInMemoryState(legacy.isRehashEnabled())
               .timeout(legacy.getRehashWaitTime());
      } else if (legacy.getCacheMode().isClustered()) { // REPL or DIST
         builder.clustering()
               .stateTransfer()
               .fetchInMemoryState(legacy.isFetchInMemoryState())
               .timeout(legacy.getStateRetrievalTimeout());
      }
      // We use the chunkSize from stateRetrieval regardless of cache mode in the legacy configuration
      builder.clustering()
         .stateTransfer()
            .chunkSize(legacy.getStateRetrievalChunkSize());

      if (legacy.getCacheMode().isSynchronous()) {
         builder.clustering()
            .sync()
               .replTimeout(legacy.getSyncReplTimeout());
      }

      for (CustomInterceptorConfig interceptor : legacy.getCustomInterceptors()) {
         InterceptorConfigurationBuilder interceptorConfigurationBuilder = builder.clustering().customInterceptors().addInterceptor();
         interceptorConfigurationBuilder.interceptor(interceptor.getInterceptor());
         if (interceptor.getAfter() != null && !interceptor.getAfter().isEmpty())
            interceptorConfigurationBuilder.after(Util.<CommandInterceptor>loadClass(interceptor.getAfter(), legacy.getClassLoader()));
         else if (interceptor.getBefore() != null && !interceptor.getBefore().isEmpty())
            interceptorConfigurationBuilder.before(Util.<CommandInterceptor>loadClass(interceptor.getBefore(), legacy.getClassLoader()));
         else if (!interceptor.getPositionAsString().equals(Position.OTHER_THAN_FIRST_OR_LAST.toString()))
            interceptorConfigurationBuilder.position(Position.valueOf(interceptor.getPositionAsString()));
         else
            interceptorConfigurationBuilder.index(interceptor.getIndex());
      }

      builder.dataContainer()
         .dataContainer(legacy.getDataContainer())
         .withProperties(legacy.getDataContainerProperties());

      if (legacy.isDeadlockDetectionEnabled()) {
         builder.deadlockDetection().enable()
            .spinDuration(legacy.getDeadlockDetectionSpinDuration());
      } else {
         builder.deadlockDetection().disable();
      }

      builder.eviction()
         .maxEntries(legacy.getEvictionMaxEntries())
         .strategy(legacy.getEvictionStrategy())
         .threadPolicy(legacy.getEvictionThreadPolicy());

      builder.expiration()
         .lifespan(legacy.getExpirationLifespan())
         .maxIdle(legacy.getExpirationMaxIdle())
         .reaperEnabled(legacy.isExpirationReaperEnabled())
         .wakeUpInterval(legacy.getExpirationWakeUpInterval());

      if (legacy.isIndexingEnabled())
         builder.indexing().enable()
            .indexLocalOnly(legacy.isIndexLocalOnly())
            .withProperties(legacy.getIndexingProperties());
      else
         builder.indexing().disable();

      if (legacy.isInvocationBatchingEnabled()) {
         builder.invocationBatching().enable();
      } else {
         builder.invocationBatching().disable();
      }

      builder.jmxStatistics().enabled(legacy.isExposeJmxStatistics());

      // TODO lazy deserialization?

      builder.loaders()
         .passivation(legacy.isCacheLoaderPassivation())
         .preload(legacy.isCacheLoaderPreload())
         .shared(legacy.isCacheLoaderShared());

      for (CacheLoaderConfig clc : legacy.getCacheLoaders()) {
         adapt(legacy.getClassLoader(), builder, clc);
      }

      builder.locking()
         .concurrencyLevel(legacy.getConcurrencyLevel())
         .isolationLevel(legacy.getIsolationLevel())
         .lockAcquisitionTimeout(legacy.getLockAcquisitionTimeout())
         .useLockStriping(legacy.isUseLockStriping())
         .writeSkewCheck(legacy.isWriteSkewCheck());

      if (legacy.isStoreAsBinary())
         builder.storeAsBinary().enable()
            .storeKeysAsBinary(legacy.isStoreKeysAsBinary())
            .storeValuesAsBinary(legacy.isStoreValuesAsBinary());
      else
         builder.storeAsBinary().disable();

      builder.transaction()
         .autoCommit(legacy.isTransactionAutoCommit())
         .cacheStopTimeout(legacy.getCacheStopTimeout())
         .eagerLockingSingleNode(legacy.isEagerLockSingleNode())
         .lockingMode(legacy.getTransactionLockingMode())
         .syncCommitPhase(legacy.isSyncCommitPhase())
         .syncRollbackPhase(legacy.isSyncRollbackPhase())
         .transactionMode(legacy.getTransactionMode())
         .transactionSynchronizationRegistryLookup(legacy.getTransactionSynchronizationRegistryLookup())
         .useEagerLocking(legacy.isUseEagerLocking())
         .useSynchronization(legacy.isUseSynchronizationForTransactions())
         .use1PcForAutoCommitTransactions(legacy.isUse1PcForAutoCommitTransactions());

      TransactionManagerLookup tmLookup = legacy.getTransactionManagerLookup();
      if (tmLookup != null) {
         builder.transaction().transactionManagerLookup(tmLookup);
      } else {
         String tmLookupClass = legacy.getTransactionManagerLookupClass();
         if (tmLookupClass != null) {
            builder.transaction().transactionManagerLookup(
               Util.<TransactionManagerLookup>getInstance(
                  tmLookupClass,Thread.currentThread().getContextClassLoader()));
         }
      }

      builder.versioning()
            .enabled(legacy.isEnableVersioning())
            .scheme(legacy.getVersioningScheme());

      builder.transaction().recovery().enabled(legacy.isTransactionRecoveryEnabled());

      builder.unsafe().unreliableReturnValues(legacy.isUnsafeUnreliableReturnValues());

      return builder.build();
   }

   // Temporary method... once cache store configs have been converted, this should go
   public static void adapt(ClassLoader cl, ConfigurationBuilder builder, CacheLoaderConfig clc) {
      LoaderConfigurationBuilder<?, ?> loaderBuilder = null;
      if (clc instanceof ClusterCacheLoaderConfig) {
         ClusterCacheLoaderConfig cclc = (ClusterCacheLoaderConfig) clc;
         ClusterCacheLoaderConfigurationBuilder cclBuilder = builder.loaders().addClusterCacheLoader();
         cclBuilder.remoteCallTimeout(cclc.getRemoteCallTimeout());
         loaderBuilder = cclBuilder;
      } else if (clc instanceof FileCacheStoreConfig) {
         FileCacheStoreConfig csc = (FileCacheStoreConfig) clc;
         FileCacheStoreConfigurationBuilder fcsBuilder = builder.loaders().addFileCacheStore();

         fcsBuilder.fetchPersistentState(csc.isFetchPersistentState());
         fcsBuilder.ignoreModifications(csc.isIgnoreModifications());
         fcsBuilder.purgeOnStartup(csc.isPurgeOnStartup());
         fcsBuilder.purgerThreads(csc.getPurgerThreads());
         fcsBuilder.purgeSynchronously(csc.isPurgeSynchronously());

         fcsBuilder.location(csc.getLocation());
         fcsBuilder.fsyncInterval(csc.getFsyncInterval());
         fcsBuilder.fsyncMode(FileCacheStoreConfigurationBuilder.FsyncMode.valueOf(csc.getFsyncMode().name()));
         fcsBuilder.streamBufferSize(csc.getStreamBufferSize());
         loaderBuilder = fcsBuilder;
      } else if (clc instanceof CacheStoreConfig) {
         LegacyStoreConfigurationBuilder tmpStoreBuilder = builder.loaders().addStore();
         tmpStoreBuilder.cacheStore(Util.<CacheStore>getInstance(clc.getCacheLoaderClassName(), cl));
         CacheStoreConfig csc = (CacheStoreConfig) clc;
         tmpStoreBuilder.fetchPersistentState(csc.isFetchPersistentState());
         tmpStoreBuilder.ignoreModifications(csc.isIgnoreModifications());
         tmpStoreBuilder.purgeOnStartup(csc.isPurgeOnStartup());
         tmpStoreBuilder.purgerThreads(csc.getPurgerThreads());
         tmpStoreBuilder.purgeSynchronously(csc.isPurgeSynchronously());
         loaderBuilder = tmpStoreBuilder;
         if (clc instanceof AbstractCacheStoreConfig) {
            tmpStoreBuilder.withProperties(((AbstractCacheLoaderConfig) clc).getProperties());
         }
      } else {
         LegacyLoaderConfigurationBuilder tmpLoaderBuilder = builder.loaders().addLoader();
         tmpLoaderBuilder.cacheLoader(Util.<CacheLoader>getInstance(clc.getCacheLoaderClassName(), cl));
         loaderBuilder = tmpLoaderBuilder;
      }
      if (clc instanceof CacheStoreConfig) {
         CacheStoreConfig csc = (CacheStoreConfig) clc;
         StoreConfigurationBuilder<?, ?> storeBuilder = (StoreConfigurationBuilder<?, ?>) loaderBuilder;
         storeBuilder.async().enabled(csc.getAsyncStoreConfig().isEnabled());
         storeBuilder.async().flushLockTimeout(csc.getAsyncStoreConfig().getFlushLockTimeout());
         storeBuilder.async().modificationQueueSize(csc.getAsyncStoreConfig().getModificationQueueSize());
         storeBuilder.async().shutdownTimeout(csc.getAsyncStoreConfig().getShutdownTimeout());
         storeBuilder.async().threadPoolSize(csc.getAsyncStoreConfig().getThreadPoolSize());
         storeBuilder.singletonStore().enabled(csc.getSingletonStoreConfig().isSingletonStoreEnabled());
         storeBuilder.singletonStore().pushStateTimeout(csc.getSingletonStoreConfig().getPushStateTimeout());
         storeBuilder.singletonStore().pushStateWhenCoordinator(csc.getSingletonStoreConfig().isPushStateWhenCoordinator());
      }
   }

}
