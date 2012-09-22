/**
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *   ~
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

package org.infinispan.spring;

import java.util.Arrays;
import java.util.List;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.config.CustomInterceptorConfig;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.JBossTransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link ConfigurationOverrides}.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 *
 */
public class ConfigurationOverridesTest {

   @Test
   public final void configurationOverridesShouldOverrideDeadlockSpinDetectionDurationPropIfExplicitlySet() throws Exception {
      final long expectedDeadlockSpinDetectionDuration = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setDeadlockDetectionSpinDuration(expectedDeadlockSpinDetectionDuration);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set deadlockDetectionSpinDuration. However, it didn't.",
                  expectedDeadlockSpinDetectionDuration,
                  configuration.deadlockDetection().spinDuration());
   }

   @Test
   public final void configurationOverridesShouldOverrideEnableDeadlockDetectionPropIfExplicitlySet()
         throws Exception {
      final boolean expectedEnableDeadlockDetection = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEnableDeadlockDetection(expectedEnableDeadlockDetection);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set enableDeadlockDetection property. However, it didn't.",
                  expectedEnableDeadlockDetection,
                  configuration.deadlockDetection().enabled());
   }

   @Test
   public final void configurationOverridesShouldOverrideUseLockStripingPropIfExplicitlySet()
         throws Exception {
      final boolean expectedUseLockStriping = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseLockStriping(expectedUseLockStriping);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set useLockStriping property. However, it didn't.",
                  expectedUseLockStriping,
                  configuration.locking().useLockStriping());
   }

   @Test
   public final void configurationOverridesShouldOverrideUnsafeUnreliableReturnValuesPropIfExplicitlySet()
         throws Exception {
      final boolean expectedUnsafeUnreliableReturnValues = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUnsafeUnreliableReturnValues(expectedUnsafeUnreliableReturnValues);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();

      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set unsafeUnreliableReturnValues property. However, it didn't.",
                  expectedUnsafeUnreliableReturnValues,
                  configuration.unsafe().unreliableReturnValues());
   }

   @Test
   public final void configurationOverridesShouldOverrideRehashRpcTimeoutPropIfExplicitlySet()
         throws Exception {
      final long expectedRehashRpcTimeout = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setRehashRpcTimeout(expectedRehashRpcTimeout);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set rehashRpcTimeout property. However, it didn't.",
                  expectedRehashRpcTimeout,
                  configuration.clustering().stateTransfer().timeout());
   }

   @Test
   public final void configurationOverridesShouldOverrideWriteSkewCheckPropIfExplicitlySet()
         throws Exception {
      final boolean expectedWriteSkewCheck = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setWriteSkewCheck(expectedWriteSkewCheck);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set writeSkewCheck property. However, it didn't.",
                  expectedWriteSkewCheck,
                  configuration.locking().writeSkewCheck());
   }

   @Test
   public final void configurationOverridesShouldOverrideConcurrencyLevelPropIfExplicitlySet()
         throws Exception {
      final int expectedConcurrencyLevel = 10000;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setConcurrencyLevel(expectedConcurrencyLevel);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set ConcurrencyLevel property. However, it didn't.",
                  expectedConcurrencyLevel,
                  configuration.locking().concurrencyLevel());
   }

   @Test
   public final void configurationOverridesShouldOverrideReplQueueMaxElementsPropIfExplicitlySet()
         throws Exception {
      final int expectedReplQueueMaxElements = 10000;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setReplQueueMaxElements(expectedReplQueueMaxElements);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set ReplQueueMaxElements property. However, it didn't.",
                  expectedReplQueueMaxElements,
                  configuration.clustering().async().replQueueMaxElements());
   }

   @Test
   public final void configurationOverridesShouldOverrideReplQueueIntervalPropIfExplicitlySet()
         throws Exception {
      final long expectedReplQueueInterval = 10000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setReplQueueInterval(expectedReplQueueInterval);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set ReplQueueInterval property. However, it didn't.",
                  expectedReplQueueInterval,
                  configuration.clustering().async().replQueueInterval());
   }

   @Test
   public final void configurationOverridesShouldOverrideReplQueueClassPropIfExplicitlySet()
         throws Exception {
      final String expectedReplQueueClass = "repl.queue.Class";//FIXME create one

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setReplQueueClass(expectedReplQueueClass);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set ReplQueueClass property. However, it didn't.",
                  expectedReplQueueClass,
                  configuration.clustering().async().replQueue().getClass());
   }

   @Test
   public final void configurationOverridesShouldOverrideExposeJmxStatisticsPropIfExplicitlySet() throws Exception {
      final boolean expectedExposeJmxStatistics = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setExposeJmxStatistics(expectedExposeJmxStatistics);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set ExposeJmxStatistics property. However, it didn't.",
                  expectedExposeJmxStatistics,
                  configuration.jmxStatistics().enabled());
   }

   @Test
   public final void configurationOverridesShouldOverrideInvocationBatchingEnabledPropIfExplicitlySet()
         throws Exception {
      final boolean expectedInvocationBatchingEnabled = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setInvocationBatchingEnabled(expectedInvocationBatchingEnabled);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set InvocationBatchingEnabled property. However, it didn't.",
                  expectedInvocationBatchingEnabled,
                  configuration.invocationBatching().enabled());
   }

   @Test
   public final void configurationOverridesShouldOverrideFetchInMemoryStatePropIfExplicitlySet()
         throws Exception {
      final boolean expectedFetchInMemoryState = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setFetchInMemoryState(expectedFetchInMemoryState);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set FetchInMemoryState property. However, it didn't.",
                  expectedFetchInMemoryState,
                  configuration.clustering().stateTransfer().fetchInMemoryState());
   }

   @Test
   public final void configurationOverridesShouldOverrideAlwaysProvideInMemoryStatePropIfExplicitlySet()
         throws Exception {
      final boolean expectedAlwaysProvideInMemoryState = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setAlwaysProvideInMemoryState(expectedAlwaysProvideInMemoryState);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set AlwaysProvideInMemoryState property. However, it didn't.",
                  expectedAlwaysProvideInMemoryState,
                  configuration.clustering().stateTransfer().fetchInMemoryState());//FIXME
   }

   @Test
   public final void configurationOverridesShouldOverrideLockAcquisitionTimeoutPropIfExplicitlySet()
         throws Exception {
      final long expectedLockAcquisitionTimeout = 1000000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setLockAcquisitionTimeout(expectedLockAcquisitionTimeout);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set LockAcquisitionTimeout property. However, it didn't.",
                  expectedLockAcquisitionTimeout,
                  configuration.locking().lockAcquisitionTimeout());
   }

   @Test
   public final void configurationOverridesShouldOverrideSyncReplTimeoutPropIfExplicitlySet()
         throws Exception {
      final long expectedSyncReplTimeout = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setSyncReplTimeout(expectedSyncReplTimeout);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set SyncReplTimeout property. However, it didn't.",
                  expectedSyncReplTimeout,
                  configuration.clustering().stateTransfer().timeout());
   }

   @Test
   public final void configurationOverridesShouldOverrideCacheModeStringPropIfExplicitlySet()
         throws Exception {
      final String expectedCacheModeString = CacheMode.LOCAL.name();

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setCacheModeString(expectedCacheModeString);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set CacheModeString property. However, it didn't.",
                  expectedCacheModeString,
                  configuration.clustering().cacheModeString());
   }

   @Test
   public final void configurationOverridesShouldOverrideEvictionWakeUpIntervalPropIfExplicitlySet()
         throws Exception {
      final long expectedExpirationWakeUpInterval = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setExpirationWakeUpInterval(expectedExpirationWakeUpInterval);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set EvictionWakeUpInterval property. However, it didn't.",
                  expectedExpirationWakeUpInterval,
                  configuration.expiration().wakeUpInterval());
   }

   @Test
   public final void configurationOverridesShouldOverrideEvictionStrategyPropIfExplicitlySet()
         throws Exception {
      final EvictionStrategy expectedEvictionStrategy = EvictionStrategy.LIRS;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionStrategy(expectedEvictionStrategy);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set EvictionStrategy property. However, it didn't.",
                  expectedEvictionStrategy,
                  configuration.eviction().strategy());
   }

   @Test
   public final void configurationOverridesShouldOverrideEvictionStrategyClassPropIfExplicitlySet()
         throws Exception {
      final String expectedEvictionStrategyClass = "LRU";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionStrategyClass(expectedEvictionStrategyClass);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set EvictionStrategyClass property. However, it didn't.",
                  EvictionStrategy.LRU,
                  configuration.eviction().strategy());
   }

   @Test
   public final void configurationOverridesShouldOverrideEvictionThreadPolicyPropIfExplicitlySet()
         throws Exception {
      final EvictionThreadPolicy expectedEvictionThreadPolicy = EvictionThreadPolicy.PIGGYBACK;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionThreadPolicy(expectedEvictionThreadPolicy);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set EvictionThreadPolicy property. However, it didn't.",
                  expectedEvictionThreadPolicy,
                  configuration.eviction().threadPolicy());
   }

   @Test
   public final void configurationOverridesShouldOverrideEvictionThreadPolicyClassPropIfExplicitlySet()
         throws Exception {
      final String expectedEvictionThreadPolicyClass = "PIGGYBACK";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionThreadPolicyClass(expectedEvictionThreadPolicyClass);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set EvictionThreadPolicyClass property. However, it didn't.",
                  EvictionThreadPolicy.PIGGYBACK,
                  configuration.eviction().threadPolicy());
   }

   @Test
   public final void configurationOverridesShouldOverrideEvictionMaxEntriesPropIfExplicitlySet()
         throws Exception {
      final int expectedEvictionMaxEntries = 1000000;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionMaxEntries(expectedEvictionMaxEntries);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set EvictionMaxEntries property. However, it didn't.",
                  expectedEvictionMaxEntries,
                  configuration.eviction().maxEntries());
   }

   @Test
   public final void configurationOverridesShouldOverrideExpirationLifespanPropIfExplicitlySet()
         throws Exception {
      final long expectedExpirationLifespan = 1000000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setExpirationLifespan(expectedExpirationLifespan);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set ExpirationLifespan property. However, it didn't.",
                  expectedExpirationLifespan,
                  configuration.expiration().lifespan());
   }

   @Test
   public final void configurationOverridesShouldOverrideExpirationMaxIdlePropIfExplicitlySet()
         throws Exception {
      final long expectedExpirationMaxIdle = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setExpirationMaxIdle(expectedExpirationMaxIdle);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set ExpirationMaxIdle property. However, it didn't.",
                  expectedExpirationMaxIdle,
                  configuration.expiration().maxIdle());
   }

   @Test
   public final void configurationOverridesShouldOverrideTransactionManagerLookupClassPropIfExplicitlySet()
         throws Exception {
      final String expectedTransactionManagerLookupClass = "expected.transaction.manager.lookup.Class";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setTransactionManagerLookupClass(expectedTransactionManagerLookupClass);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set TransactionManagerLookupClass property. However, it didn't.",
                  expectedTransactionManagerLookupClass,
                  configuration.transaction().transactionManagerLookup());
   }

   @Test
   public final void configurationOverridesShouldOverrideTransactionManagerLookupPropIfExplicitlySet()
         throws Exception {
      final TransactionManagerLookup expectedTransactionManagerLookup = new JBossTransactionManagerLookup();

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setTransactionManagerLookup(expectedTransactionManagerLookup);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set TransactionManagerLookup property. However, it didn't.",
                  expectedTransactionManagerLookup,
                  configuration.transaction().transactionManagerLookup());
   }

   @Test
   public final void configurationOverridesShouldOverrideCacheLoaderManagerConfigPropIfExplicitlySet()
         throws Exception {
      final CacheLoaderManagerConfig expectedCacheLoaderManagerConfig = new CacheLoaderManagerConfig();

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setCacheLoaderManagerConfig(expectedCacheLoaderManagerConfig);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertSame(
                  "ConfigurationOverrides should have overridden default value with explicitly set CacheLoaderManagerConfig property. However, it didn't.",
                  expectedCacheLoaderManagerConfig,
                  configuration.loaders().cacheLoaders());//FIXME
   }

   @Test
   public final void configurationOverridesShouldOverrideSyncCommitPhasePropIfExplicitlySet()
         throws Exception {
      final boolean expectedSyncCommitPhase = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setSyncCommitPhase(expectedSyncCommitPhase);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set SyncCommitPhase property. However, it didn't.",
                  expectedSyncCommitPhase,
                  configuration.transaction().syncCommitPhase());
   }

   @Test
   public final void configurationOverridesShouldOverrideSyncRollbackPhasePropIfExplicitlySet()
         throws Exception {
      final boolean expectedSyncRollbackPhase = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setSyncRollbackPhase(expectedSyncRollbackPhase);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set SyncRollbackPhase property. However, it didn't.",
                  expectedSyncRollbackPhase,
                  configuration.transaction().syncRollbackPhase());
   }

   @Test
   public final void configurationOverridesShouldOverrideUseEagerLockingPropIfExplicitlySet()
         throws Exception {

      final LockingMode expectedLockingMode = LockingMode.PESSIMISTIC;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseEagerLocking(true);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set UseEagerLocking property. However, it didn't.",
                  expectedLockingMode,
                  configuration.transaction().lockingMode());
   }

   @Test
   public final void configurationOverridesShouldOverrideUseReplQueuePropIfExplicitlySet() throws Exception {
      final boolean expectedUseReplQueue = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseReplQueue(expectedUseReplQueue);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set UseReplQueue property. However, it didn't.",
                  expectedUseReplQueue,
                  configuration.clustering().async().useReplQueue());
   }

   @Test
   public final void configurationOverridesShouldOverrideIsolationLevelPropIfExplicitlySet()
         throws Exception {
      final IsolationLevel expectedIsolationLevel = IsolationLevel.SERIALIZABLE;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setIsolationLevel(expectedIsolationLevel);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set IsolationLevel property. However, it didn't.",
                  expectedIsolationLevel,
                  configuration.locking().isolationLevel());
   }

   @Test
   public final void configurationOverridesShouldOverrideStateRetrievalTimeoutPropIfExplicitlySet()
         throws Exception {
      final long expectedStateRetrievalTimeout = 1000000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setStateRetrievalTimeout(expectedStateRetrievalTimeout);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set StateRetrievalTimeout property. However, it didn't.",
                  expectedStateRetrievalTimeout,
                  configuration.clustering().stateTransfer().timeout());
   }

   @Test
   public final void configurationOverridesShouldOverrideStateRetrievalChunkSizePropIfExplicitlySet()
         throws Exception {
      final int expectedStateRetrievalChunkSize = 123456;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest
            .setStateRetrievalChunkSize(expectedStateRetrievalChunkSize);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set StateRetrievalChunkSize property. However, it didn't.",
                  expectedStateRetrievalChunkSize,
                  configuration.clustering().stateTransfer().chunkSize());
   }

   @Test
   public final void configurationOverridesShouldOverrideIsolationLevelClassPropIfExplicitlySet()
         throws Exception {
      final String expectedIsolationLevelClass = "REPEATABLE_READ";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setIsolationLevelClass(expectedIsolationLevelClass);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set IsolationLevelClass property. However, it didn't.",
                  IsolationLevel.REPEATABLE_READ, configuration.locking().isolationLevel());
   }

   @Test
   public final void configurationOverridesShouldOverrideUseLazyDeserializationPropIfExplicitlySet()
         throws Exception {
      final boolean expectedUseLazyDeserialization = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseLazyDeserialization(expectedUseLazyDeserialization);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set UseLazyDeserialization property. However, it didn't.",
                  expectedUseLazyDeserialization, configuration.storeAsBinary().enabled());
   }

   @Test
   public final void configurationOverridesShouldOverrideL1CacheEnabledPropIfExplicitlySet()
         throws Exception {
      final boolean expectedL1CacheEnabled = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setL1CacheEnabled(expectedL1CacheEnabled);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set L1CacheEnabled property. However, it didn't.",
                  expectedL1CacheEnabled, configuration.clustering().l1().enabled());
   }

   @Test
   public final void configurationOverridesShouldOverrideL1LifespanPropIfExplicitlySet()
         throws Exception {
      final long expectedL1Lifespan = 2300446L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setL1Lifespan(expectedL1Lifespan);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set L1Lifespan property. However, it didn't.",
                  expectedL1Lifespan, configuration.clustering().l1().lifespan());
   }

   @Test
   public final void configurationOverridesShouldOverrideL1OnRehashPropIfExplicitlySet()
         throws Exception {
      final boolean expectedL1OnRehash = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setL1OnRehash(expectedL1OnRehash);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set L1OnRehash property. However, it didn't.",
                  expectedL1OnRehash, configuration.clustering().l1().onRehash());
   }

   @Test
   public final void configurationOverridesShouldOverrideConsistentHashClassPropIfExplicitlySet()
         throws Exception {
      final String expectedConsistentHashClass = "expected.consistent.hash.Class";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setConsistentHashClass(expectedConsistentHashClass);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set ConsistentHashClass property. However, it didn't.",
                  expectedConsistentHashClass, configuration.clustering().hash().consistentHashFactory().getClass().getName());
   }

   @Test
   public final void configurationOverridesShouldOverrideNumOwnersPropIfExplicitlySet()
         throws Exception {
      final int expectedNumOwners = 675443;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setNumOwners(expectedNumOwners);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set NumOwners property. However, it didn't.",
                  expectedNumOwners, configuration.clustering().hash().numOwners());
   }

   @Test
   public final void configurationOverridesShouldOverrideRehashEnabledPropIfExplicitlySet()
         throws Exception {
      final boolean expectedRehashEnabled = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setRehashEnabled(expectedRehashEnabled);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set RehashEnabled property. However, it didn't.",
                  expectedRehashEnabled, configuration.clustering().stateTransfer().fetchInMemoryState());
   }

   @Test
   public final void configurationOverridesShouldOverrideRehashWaitTimePropIfExplicitlySet()
         throws Exception {
      final long expectedRehashWaitTime = 1232778L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setRehashWaitTime(expectedRehashWaitTime);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set RehashWaitTime property. However, it didn't.",
                  expectedRehashWaitTime, configuration.clustering().stateTransfer().timeout());
   }

   @Test
   public final void configurationOverridesShouldOverrideUseAsyncMarshallingPropIfExplicitlySet()
         throws Exception {
      final boolean expectedUseAsyncMarshalling = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseAsyncMarshalling(expectedUseAsyncMarshalling);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set UseAsyncMarshalling property. However, it didn't.",
                  expectedUseAsyncMarshalling, configuration.clustering().async().asyncMarshalling());
   }

   @Test
   public final void configurationOverridesShouldOverrideIndexingEnabledPropIfExplicitlySet()
         throws Exception {
      final boolean expectedIndexingEnabled = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setIndexingEnabled(expectedIndexingEnabled);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set IndexingEnabled property. However, it didn't.",
                  expectedIndexingEnabled, configuration.indexing().enabled());
   }

   @Test
   public final void configurationOverridesShouldOverrideIndexLocalOnlyPropIfExplicitlySet()
         throws Exception {
      final boolean expectedIndexLocalOnly = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setIndexLocalOnly(expectedIndexLocalOnly);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set IndexLocalOnly property. However, it didn't.",
                  expectedIndexLocalOnly, configuration.indexing().indexLocalOnly());
   }

   @Test
   public final void configurationOverridesShouldOverrideCustomInterceptorsPropIfExplicitlySet()
         throws Exception {
      final CustomInterceptorConfig customInterceptor = new CustomInterceptorConfig();
      final List<CustomInterceptorConfig> expectedCustomInterceptors = Arrays
            .asList(customInterceptor);

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setCustomInterceptors(expectedCustomInterceptors);
      final ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      objectUnderTest.applyOverridesTo(defaultConfiguration);
      Configuration configuration = defaultConfiguration.build();

      AssertJUnit
            .assertEquals(
                  "ConfigurationOverrides should have overridden default value with explicitly set CustomInterceptors property. However, it didn't.",
                  expectedCustomInterceptors, configuration.customInterceptors().interceptors());
   }
}
