/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.config;

import java.util.List;
import java.util.Properties;

import org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.interceptors.*;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.jmx.JBossMBeanServerLookup;
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.marshall.exts.ArrayListExternalizer;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.hash.MurmurHash3;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Programmatic configuration test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "config.ProgrammaticConfigurationTest")
public class ProgrammaticConfigurationTest extends AbstractInfinispanTest {

   public void testGlobalConfiguration() {
      GlobalConfiguration gc = new GlobalConfiguration().fluent()
         .transport()
            .clusterName("boo").distributedSyncTimeout(999L)
            .addProperty("configurationFile", "jgroups-tcp.xml")
            .machineId("id").rackId("rack").strictPeerToPeer(true)
         .globalJmxStatistics()
            .jmxDomain("org.my.infinispan")
            .allowDuplicateDomains(true).cacheManagerName("BooMooCacheManager")
            .mBeanServerLookupClass(JBossMBeanServerLookup.class)
            .withProperties(new TypedProperties()
                                  .setProperty("jb", "oss"))
            .addProperty("na", "me")
         .serialization()
            .version("2.2").marshallerClass(VersionAwareMarshaller.class)
            .addAdvancedExternalizer(ReplicableCommandExternalizer.class)
            .addAdvancedExternalizer(999, ArrayListExternalizer.class)
         .asyncListenerExecutor()
            .factory(DefaultExecutorFactory.class)
            .addProperty("maxThreads", "6")
            .addProperty("threadNamePrefix", "AsyncListenerThread2")
         .asyncTransportExecutor()
            .factory(DefaultExecutorFactory.class)
            .withProperties(new TypedProperties()
                                  .setProperty("maxThreads", "26")
                                  .setProperty("threadNamePrefix", "AsyncSerializationThread2"))
         .evictionScheduledExecutor()
            .factory(DefaultScheduledExecutorFactory.class)
            .addProperty("threadNamePrefix", "EvictionThread2")
         .replicationQueueScheduledExecutor()
            .factory(DefaultScheduledExecutorFactory.class)
            .addProperty("threadNamePrefix", "ReplicationQueueThread2")
         .shutdown()
            .hookBehavior(ShutdownHookBehavior.DONT_REGISTER)
         .build()
      ;

      assertEquals("boo", gc.getClusterName());
      assertEquals(999L, gc.getDistributedSyncTimeout());
      assertEquals("jgroups-tcp.xml", gc.getTransportProperties().getProperty("configurationFile"));
      assertEquals("rack", gc.getRackId());
      assertEquals("id", gc.getMachineId());
      assert gc.isStrictPeerToPeer();

      assertEquals("org.my.infinispan", gc.getJmxDomain());
      assert gc.isExposeGlobalJmxStatistics();
      assert gc.isAllowDuplicateDomains();
      assertEquals("BooMooCacheManager", gc.getCacheManagerName());
      assertEquals(JBossMBeanServerLookup.class.getName(), gc.getMBeanServerLookup());
      assertEquals("oss", gc.getMBeanServerProperties().getProperty("jb"));
      assertEquals("me", gc.getMBeanServerProperties().getProperty("na"));

      assertEquals("2.2", gc.getMarshallVersionString());
      List<AdvancedExternalizerConfig> exts = gc.getExternalizers();
      assertEquals(2, exts.size());
      assertEquals(ReplicableCommandExternalizer.class.getName(), exts.get(0).getExternalizerClass());
      assertEquals(ArrayListExternalizer.class.getName(), exts.get(1).getExternalizerClass());
      assert 999 == exts.get(1).getId();

      assertEquals(DefaultExecutorFactory.class.getName(), gc.getAsyncListenerExecutorFactoryClass());
      Properties asyncListenerExecutorProps = gc.getAsyncListenerExecutorProperties();
      assertEquals("6", asyncListenerExecutorProps.getProperty("maxThreads"));
      assertEquals("AsyncListenerThread2", asyncListenerExecutorProps.getProperty("threadNamePrefix"));

      assertEquals(DefaultExecutorFactory.class.getName(), gc.getAsyncTransportExecutorFactoryClass());
      Properties asyncTransportExecutorProps = gc.getAsyncTransportExecutorProperties();
      assertEquals("26", asyncTransportExecutorProps.getProperty("maxThreads"));
      assertEquals("AsyncSerializationThread2", asyncTransportExecutorProps.getProperty("threadNamePrefix"));

      assertEquals(DefaultScheduledExecutorFactory.class.getName(), gc.getEvictionScheduledExecutorFactoryClass());
      Properties evictionScheduledExecutorProps = gc.getEvictionScheduledExecutorProperties();
      assertEquals("EvictionThread2", evictionScheduledExecutorProps.getProperty("threadNamePrefix"));

      assertEquals(DefaultScheduledExecutorFactory.class.getName(), gc.getReplicationQueueScheduledExecutorFactoryClass());
      Properties replicationQueueScheduledExecutorProps = gc.getReplicationQueueScheduledExecutorProperties();
      assertEquals("ReplicationQueueThread2", replicationQueueScheduledExecutorProps.getProperty("threadNamePrefix"));

      assert ShutdownHookBehavior.DONT_REGISTER == gc.getShutdownHookBehavior();

      gc = new GlobalConfiguration();
      assert !gc.isExposeGlobalJmxStatistics();
   }

   public void testConfiguration() {
      Configuration c = new Configuration().fluent()
         .locking()
            .concurrencyLevel(1234).isolationLevel(IsolationLevel.SERIALIZABLE)
            .lockAcquisitionTimeout(8888L).useLockStriping(false).writeSkewCheck(true)
         .loaders()
            .shared(true).passivation(false)
            .addCacheLoader(
               new FileCacheStoreConfig()
                  .purgeOnStartup(true)
                  .location("/tmp2").streamBufferSize(1615)
                  .asyncStore()
                     .threadPoolSize(14).flushLockTimeout(777L)
                     .shutdownTimeout(666L)
                  .fetchPersistentState(false).ignoreModifications(true)
                  .singletonStore()
                     .pushStateWhenCoordinator(true).pushStateTimeout(8989L)
                  .purgeSynchronously(false))
            .addCacheLoader(
               new DummyInMemoryCacheStore.Cfg()
                  .debug(true)
                  .failKey("fail")
                  .purgeOnStartup(false)
                  .asyncStore()
                     .threadPoolSize(21)
                  .purgeSynchronously(true))
            .addCacheLoader(
               new ClusterCacheLoaderConfig().remoteCallTimeout(7694L))
            .preload(true)
         .transaction()
            .cacheStopTimeout(1928).eagerLockSingleNode(false)
            .syncCommitPhase(true).syncRollbackPhase(false).useEagerLocking(false)
            .recovery()
               .recoveryInfoCacheName("mmmmmircea")
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .useSynchronization(true)
         .deadlockDetection()
            .spinDuration(8373L)
         .customInterceptors()
            .add(new OptimisticLockingInterceptor()).first()
            .add(new DistributionInterceptor()).last()
            .add(new CallInterceptor()).atIndex(8)
            .add(new CacheStoreInterceptor()).after(OptimisticLockingInterceptor.class)
            .add(new CacheLoaderInterceptor()).before(CallInterceptor.class)
         .eviction()
            .maxEntries(7676).strategy(EvictionStrategy.FIFO)
            .threadPolicy(EvictionThreadPolicy.PIGGYBACK)
         .expiration()
            .maxIdle(8392L).lifespan(4372L).wakeUpInterval(7585L)
         .clustering()
            .mode(Configuration.CacheMode.INVALIDATION_SYNC)
            .async()
               .replQueueClass(ReplicationQueueImpl.class)
               .asyncMarshalling(false)
               .replQueueInterval(5738L)
               .replQueueMaxElements(19191)
               .useReplQueue(true)
            .l1()
               .lifespan(65738L).onRehash(true)
            .stateRetrieval()
               .alwaysProvideInMemoryState(false).fetchInMemoryState(true)
               .initialRetryWaitTime(8989L).retryWaitTimeIncreaseFactor(4)
               .numRetries(8).logFlushTimeout(533L).maxNonProgressingLogWrites(434)
               .timeout(7383L)
            .hash()
               .hashFunctionClass(MurmurHash3.class)
               .consistentHashClass(DefaultConsistentHash.class)
               .numOwners(200).rehashWait(74843L).rehashRpcTimeout(374L)
               .rehashEnabled(false)
         .dataContainer()
            .dataContainerClass(DefaultDataContainer.class)
            .dataContainer(new QueryableDataContainer())
            .addProperty("a-property", "a-value")
         .indexing()
            .indexLocalOnly(true)
            .addProperty("indexing", "in memory")
         .unsafe()
            .unreliableReturnValues(false)
         .jmxStatistics()
         .storeAsBinary()
         .invocationBatching()
         .build();

      assert c.isInvocationBatchingEnabled();
      assert c.isStoreAsBinary();
      assert c.isExposeJmxStatistics();
      assert !c.isUnsafeUnreliableReturnValues();

      assert c.getDataContainer() instanceof QueryableDataContainer;
      assertEquals(DefaultDataContainer.class.getName(), c.getDataContainerClass());
      assertEquals("a-value", c.getDataContainerProperties().getProperty("a-property"));

      assert c.isIndexingEnabled();
      assert c.isIndexLocalOnly();
      assert c.getIndexingProperties().getProperty("indexing").equals("in memory");

      assert !c.isAlwaysProvideInMemoryState();
      assert c.isFetchInMemoryState();
      assert 8989L == c.getStateRetrievalInitialRetryWaitTime();
      assert 4 == c.getStateRetrievalRetryWaitTimeIncreaseFactor();
      assert 8 == c.getStateRetrievalNumRetries();
      assert 533L == c.getStateRetrievalLogFlushTimeout();
      assert 434 == c.getStateRetrievalMaxNonProgressingLogWrites();
      assert 7383L == c.getStateRetrievalTimeout();
      assertEquals(MurmurHash3.class.getName(), c.getHashFunctionClass());
      assertEquals(DefaultConsistentHash.class.getName(), c.getConsistentHashClass());
      assert 200 == c.getNumOwners();
      assert 74843L == c.getRehashWaitTime();
      assert 374L == c.getRehashRpcTimeout();
      assert !c.isRehashEnabled();

      assert c.isL1CacheEnabled();
      assert c.isL1OnRehash();
      assert 65738L == c.getL1Lifespan();
      assert Configuration.CacheMode.INVALIDATION_SYNC == c.getCacheMode();
      assert !c.isUseAsyncMarshalling();
      assertEquals(ReplicationQueueImpl.class.getName(), c.getReplQueueClass());
      assert 5738L == c.getReplQueueInterval();
      assert 19191 == c.getReplQueueMaxElements();
      assert c.isUseReplQueue();

      assert 4372L == c.getExpirationLifespan();
      assert 8392L == c.getExpirationMaxIdle();

      assert 7676 == c.getEvictionMaxEntries();
      assert EvictionStrategy.FIFO == c.getEvictionStrategy();
      assert EvictionThreadPolicy.PIGGYBACK == c.getEvictionThreadPolicy();
      assert 7585L == c.getExpirationWakeUpInterval();

      List<CustomInterceptorConfig> customInterceptors = c.getCustomInterceptors();
      assert customInterceptors.get(0).getInterceptor() instanceof OptimisticLockingInterceptor;
      assert customInterceptors.get(1).getInterceptor() instanceof DistributionInterceptor;
      assert customInterceptors.get(2).getInterceptor() instanceof CallInterceptor;
      assert customInterceptors.get(3).getInterceptor() instanceof CacheStoreInterceptor;
      assert customInterceptors.get(4).getInterceptor() instanceof CacheLoaderInterceptor;

      assert c.isDeadlockDetectionEnabled();
      assert 8373L == c.getDeadlockDetectionSpinDuration();

      assert 1928 == c.getCacheStopTimeout();
      assert !c.isEagerLockSingleNode();
      assert c.isSyncCommitPhase();
      assert !c.isSyncRollbackPhase();
      assert !c.isUseEagerLocking();
      assert c.getTransactionManagerLookup() instanceof DummyTransactionManagerLookup;
      assert c.isTransactionRecoveryEnabled();
      assertEquals("mmmmmircea", c.getTransactionRecoveryCacheName());
      assert c.isUseSynchronizationForTransactions();

      ClusterCacheLoaderConfig clusterLoaderConfig = (ClusterCacheLoaderConfig) c.getCacheLoaders().get(2);
      assert 7694L == clusterLoaderConfig.getRemoteCallTimeout();

      DummyInMemoryCacheStore.Cfg dummyStoreConfig = (DummyInMemoryCacheStore.Cfg) c.getCacheLoaders().get(1);
      assert dummyStoreConfig.isDebug();
      assert !dummyStoreConfig.isPurgeOnStartup();
      assert dummyStoreConfig.isPurgeSynchronously();
      assert 21 == dummyStoreConfig.getAsyncStoreConfig().getThreadPoolSize();
      assert dummyStoreConfig.isPurgeSynchronously();

      FileCacheStoreConfig storeConfig = (FileCacheStoreConfig) c.getCacheLoaders().get(0);
      assertEquals("/tmp2", storeConfig.getLocation());
      assert 1615 == storeConfig.getStreamBufferSize();
      assert storeConfig.isPurgeOnStartup();
      assert 14 == storeConfig.getAsyncStoreConfig().getThreadPoolSize();
      assert 777L == storeConfig.getAsyncStoreConfig().getFlushLockTimeout();
      assert 666L == storeConfig.getAsyncStoreConfig().getShutdownTimeout();
      assert !storeConfig.isFetchPersistentState();
      assert storeConfig.isIgnoreModifications();
      assert storeConfig.getSingletonStoreConfig().isPushStateWhenCoordinator();
      assert 8989L == storeConfig.getSingletonStoreConfig().getPushStateTimeout();
      assert !storeConfig.isPurgeSynchronously();

      assert c.isCacheLoaderShared();
      assert !c.isCacheLoaderPassivation();
      assert c.isCacheLoaderPreload();
      assert !c.isFetchPersistentState();

      assert 1234 == c.getConcurrencyLevel();
      assert IsolationLevel.SERIALIZABLE == c.getIsolationLevel();
      assert 8888L == c.getLockAcquisitionTimeout();
      assert !c.isUseLockStriping();
      assert c.isWriteSkewCheck();
   }

}
