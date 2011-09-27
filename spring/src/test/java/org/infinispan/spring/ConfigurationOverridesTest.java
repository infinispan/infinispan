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
import org.infinispan.config.Configuration;
import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.config.CustomInterceptorConfig;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
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

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideDeadlockSpinDetectionDurationPropIfExplicitlySet()
            throws Exception {
      final long expectedDeadlockSpinDetectionDuration = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setDeadlockDetectionSpinDuration(expectedDeadlockSpinDetectionDuration);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set deadlockDetectionSpinDuration. However, it didn't.",
                        expectedDeadlockSpinDetectionDuration,
                        defaultConfiguration.getDeadlockDetectionSpinDuration());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideEnableDeadlockDetectionPropIfExplicitlySet()
            throws Exception {
      final boolean expectedEnableDeadlockDetection = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEnableDeadlockDetection(expectedEnableDeadlockDetection);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set enableDeadlockDetection property. However, it didn't.",
                        expectedEnableDeadlockDetection,
                        defaultConfiguration.isDeadlockDetectionEnabled());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideUseLockStripingPropIfExplicitlySet()
            throws Exception {
      final boolean expectedUseLockStriping = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseLockStriping(expectedUseLockStriping);
      final Configuration defaultConfiguration = new Configuration();

      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set useLockStriping property. However, it didn't.",
                        expectedUseLockStriping, defaultConfiguration.isUseLockStriping());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideUnsafeUnreliableReturnValuesPropIfExplicitlySet()
            throws Exception {
      final boolean expectedUnsafeUnreliableReturnValues = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUnsafeUnreliableReturnValues(expectedUnsafeUnreliableReturnValues);
      final Configuration defaultConfiguration = new Configuration();

      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set unsafeUnreliableReturnValues property. However, it didn't.",
                        expectedUnsafeUnreliableReturnValues,
                        defaultConfiguration.isUnsafeUnreliableReturnValues());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideRehashRpcTimeoutPropIfExplicitlySet()
            throws Exception {
      final long expectedRehashRpcTimeout = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setRehashRpcTimeout(expectedRehashRpcTimeout);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set rehashRpcTimeout property. However, it didn't.",
                        expectedRehashRpcTimeout, defaultConfiguration.getRehashRpcTimeout());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideWriteSkewCheckPropIfExplicitlySet()
            throws Exception {
      final boolean expectedWriteSkewCheck = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setWriteSkewCheck(expectedWriteSkewCheck);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set writeSkewCheck property. However, it didn't.",
                        expectedWriteSkewCheck, defaultConfiguration.isWriteSkewCheck());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideConcurrencyLevelPropIfExplicitlySet()
            throws Exception {
      final int expectedConcurrencyLevel = 10000;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setConcurrencyLevel(expectedConcurrencyLevel);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set ConcurrencyLevel property. However, it didn't.",
                        expectedConcurrencyLevel, defaultConfiguration.getConcurrencyLevel());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideReplQueueMaxElementsPropIfExplicitlySet()
            throws Exception {
      final int expectedReplQueueMaxElements = 10000;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setReplQueueMaxElements(expectedReplQueueMaxElements);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set ReplQueueMaxElements property. However, it didn't.",
                        expectedReplQueueMaxElements,
                        defaultConfiguration.getReplQueueMaxElements());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideReplQueueIntervalPropIfExplicitlySet()
            throws Exception {
      final long expectedReplQueueInterval = 10000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setReplQueueInterval(expectedReplQueueInterval);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set ReplQueueInterval property. However, it didn't.",
                        expectedReplQueueInterval, defaultConfiguration.getReplQueueInterval());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setReplQueueClass(java.lang.String)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideReplQueueClassPropIfExplicitlySet()
            throws Exception {
      final String expectedReplQueueClass = "repl.queue.Class";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setReplQueueClass(expectedReplQueueClass);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set ReplQueueClass property. However, it didn't.",
                        expectedReplQueueClass, defaultConfiguration.getReplQueueClass());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideExposeJmxStatisticsPropIfExplicitlySet()
            throws Exception {
      final boolean expectedExposeJmxStatistics = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setExposeJmxStatistics(expectedExposeJmxStatistics);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set ExposeJmxStatistics property. However, it didn't.",
                        expectedExposeJmxStatistics, defaultConfiguration.isExposeJmxStatistics());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideInvocationBatchingEnabledPropIfExplicitlySet()
            throws Exception {
      final boolean expectedInvocationBatchingEnabled = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setInvocationBatchingEnabled(expectedInvocationBatchingEnabled);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set InvocationBatchingEnabled property. However, it didn't.",
                        expectedInvocationBatchingEnabled,
                        defaultConfiguration.isInvocationBatchingEnabled());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideFetchInMemoryStatePropIfExplicitlySet()
            throws Exception {
      final boolean expectedFetchInMemoryState = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setFetchInMemoryState(expectedFetchInMemoryState);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set FetchInMemoryState property. However, it didn't.",
                        expectedFetchInMemoryState, defaultConfiguration.isFetchInMemoryState());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideAlwaysProvideInMemoryStatePropIfExplicitlySet()
            throws Exception {
      final boolean expectedAlwaysProvideInMemoryState = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setAlwaysProvideInMemoryState(expectedAlwaysProvideInMemoryState);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set AlwaysProvideInMemoryState property. However, it didn't.",
                        expectedAlwaysProvideInMemoryState,
                        defaultConfiguration.isAlwaysProvideInMemoryState());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideLockAcquisitionTimeoutPropIfExplicitlySet()
            throws Exception {
      final long expectedLockAcquisitionTimeout = 1000000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setLockAcquisitionTimeout(expectedLockAcquisitionTimeout);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set LockAcquisitionTimeout property. However, it didn't.",
                        expectedLockAcquisitionTimeout,
                        defaultConfiguration.getLockAcquisitionTimeout());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideSyncReplTimeoutPropIfExplicitlySet()
            throws Exception {
      final long expectedSyncReplTimeout = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setSyncReplTimeout(expectedSyncReplTimeout);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set SyncReplTimeout property. However, it didn't.",
                        expectedSyncReplTimeout, defaultConfiguration.getSyncReplTimeout());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setCacheModeString(java.lang.String)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideCacheModeStringPropIfExplicitlySet()
            throws Exception {
      final String expectedCacheModeString = CacheMode.LOCAL.name();

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setCacheModeString(expectedCacheModeString);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set CacheModeString property. However, it didn't.",
                        expectedCacheModeString, defaultConfiguration.getCacheModeString());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideEvictionWakeUpIntervalPropIfExplicitlySet()
            throws Exception {
      final long expectedExpirationWakeUpInterval = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setExpirationWakeUpInterval(expectedExpirationWakeUpInterval);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set EvictionWakeUpInterval property. However, it didn't.",
                        expectedExpirationWakeUpInterval,
                        defaultConfiguration.getExpirationWakeUpInterval());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setEvictionStrategy(org.infinispan.eviction.EvictionStrategy)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideEvictionStrategyPropIfExplicitlySet()
            throws Exception {
      final EvictionStrategy expectedEvictionStrategy = EvictionStrategy.LIRS;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionStrategy(expectedEvictionStrategy);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set EvictionStrategy property. However, it didn't.",
                        expectedEvictionStrategy, defaultConfiguration.getEvictionStrategy());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setEvictionStrategyClass(java.lang.String)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideEvictionStrategyClassPropIfExplicitlySet()
            throws Exception {
      final String expectedEvictionStrategyClass = "FIFO";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionStrategyClass(expectedEvictionStrategyClass);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set EvictionStrategyClass property. However, it didn't.",
                        EvictionStrategy.FIFO, defaultConfiguration.getEvictionStrategy());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setEvictionThreadPolicy(org.infinispan.eviction.EvictionThreadPolicy)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideEvictionThreadPolicyPropIfExplicitlySet()
            throws Exception {
      final EvictionThreadPolicy expectedEvictionThreadPolicy = EvictionThreadPolicy.PIGGYBACK;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionThreadPolicy(expectedEvictionThreadPolicy);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set EvictionThreadPolicy property. However, it didn't.",
                        expectedEvictionThreadPolicy,
                        defaultConfiguration.getEvictionThreadPolicy());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setEvictionThreadPolicyClass(java.lang.String)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideEvictionThreadPolicyClassPropIfExplicitlySet()
            throws Exception {
      final String expectedEvictionThreadPolicyClass = "PIGGYBACK";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionThreadPolicyClass(expectedEvictionThreadPolicyClass);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set EvictionThreadPolicyClass property. However, it didn't.",
                        EvictionThreadPolicy.PIGGYBACK,
                        defaultConfiguration.getEvictionThreadPolicy());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideEvictionMaxEntriesPropIfExplicitlySet()
            throws Exception {
      final int expectedEvictionMaxEntries = 1000000;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEvictionMaxEntries(expectedEvictionMaxEntries);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set EvictionMaxEntries property. However, it didn't.",
                        expectedEvictionMaxEntries, defaultConfiguration.getEvictionMaxEntries());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideExpirationLifespanPropIfExplicitlySet()
            throws Exception {
      final long expectedExpirationLifespan = 1000000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setExpirationLifespan(expectedExpirationLifespan);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set ExpirationLifespan property. However, it didn't.",
                        expectedExpirationLifespan, defaultConfiguration.getExpirationLifespan());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideExpirationMaxIdlePropIfExplicitlySet()
            throws Exception {
      final long expectedExpirationMaxIdle = 100000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setExpirationMaxIdle(expectedExpirationMaxIdle);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set ExpirationMaxIdle property. However, it didn't.",
                        expectedExpirationMaxIdle, defaultConfiguration.getExpirationMaxIdle());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setTransactionManagerLookupClass(java.lang.String)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideTransactionManagerLookupClassPropIfExplicitlySet()
            throws Exception {
      final String expectedTransactionManagerLookupClass = "expected.transaction.manager.lookup.Class";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setTransactionManagerLookupClass(expectedTransactionManagerLookupClass);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set TransactionManagerLookupClass property. However, it didn't.",
                        expectedTransactionManagerLookupClass,
                        defaultConfiguration.getTransactionManagerLookupClass());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setTransactionManagerLookup(org.infinispan.transaction.lookup.TransactionManagerLookup)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideTransactionManagerLookupPropIfExplicitlySet()
            throws Exception {
      final TransactionManagerLookup expectedTransactionManagerLookup = new JBossTransactionManagerLookup();

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setTransactionManagerLookup(expectedTransactionManagerLookup);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set TransactionManagerLookup property. However, it didn't.",
                        expectedTransactionManagerLookup,
                        defaultConfiguration.getTransactionManagerLookup());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setCacheLoaderManagerConfig(org.infinispan.config.CacheLoaderManagerConfig)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideCacheLoaderManagerConfigPropIfExplicitlySet()
            throws Exception {
      final CacheLoaderManagerConfig expectedCacheLoaderManagerConfig = new CacheLoaderManagerConfig();

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setCacheLoaderManagerConfig(expectedCacheLoaderManagerConfig);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertSame(
                        "ConfigurationOverrides should have overridden default value with explicitly set CacheLoaderManagerConfig property. However, it didn't.",
                        expectedCacheLoaderManagerConfig,
                        defaultConfiguration.getCacheLoaderManagerConfig());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideSyncCommitPhasePropIfExplicitlySet()
            throws Exception {
      final boolean expectedSyncCommitPhase = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setSyncCommitPhase(expectedSyncCommitPhase);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set SyncCommitPhase property. However, it didn't.",
                        expectedSyncCommitPhase, defaultConfiguration.isSyncCommitPhase());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideSyncRollbackPhasePropIfExplicitlySet()
            throws Exception {
      final boolean expectedSyncRollbackPhase = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setSyncRollbackPhase(expectedSyncRollbackPhase);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set SyncRollbackPhase property. However, it didn't.",
                        expectedSyncRollbackPhase, defaultConfiguration.isSyncRollbackPhase());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideUseEagerLockingPropIfExplicitlySet()
            throws Exception {
      final boolean expectedUseEagerLocking = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseEagerLocking(expectedUseEagerLocking);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set UseEagerLocking property. However, it didn't.",
                        expectedUseEagerLocking, defaultConfiguration.isUseEagerLocking());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideEagerLockSingleNodePropIfExplicitlySet()
            throws Exception {
      final boolean expectedEagerLockSingleNode = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setEagerLockSingleNode(expectedEagerLockSingleNode);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set EagerLockSingleNode property. However, it didn't.",
                        expectedEagerLockSingleNode, defaultConfiguration.isEagerLockSingleNode());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideUseReplQueuePropIfExplicitlySet()
            throws Exception {
      final boolean expectedUseReplQueue = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseReplQueue(expectedUseReplQueue);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set UseReplQueue property. However, it didn't.",
                        expectedUseReplQueue, defaultConfiguration.isUseReplQueue());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setIsolationLevel(org.infinispan.util.concurrent.IsolationLevel)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideIsolationLevelPropIfExplicitlySet()
            throws Exception {
      final IsolationLevel expectedIsolationLevel = IsolationLevel.SERIALIZABLE;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setIsolationLevel(expectedIsolationLevel);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set IsolationLevel property. However, it didn't.",
                        expectedIsolationLevel, defaultConfiguration.getIsolationLevel());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideStateRetrievalTimeoutPropIfExplicitlySet()
            throws Exception {
      final long expectedStateRetrievalTimeout = 1000000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setStateRetrievalTimeout(expectedStateRetrievalTimeout);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set StateRetrievalTimeout property. However, it didn't.",
                        expectedStateRetrievalTimeout,
                        defaultConfiguration.getStateRetrievalTimeout());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideStateRetrievalLogFlushTimeoutPropIfExplicitlySet()
            throws Exception {
      final long expectedStateRetrievalLogFlushTimeout = 1000000L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setStateRetrievalLogFlushTimeout(expectedStateRetrievalLogFlushTimeout);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set StateRetrievalLogFlushTimeout property. However, it didn't.",
                        expectedStateRetrievalLogFlushTimeout,
                        defaultConfiguration.getStateRetrievalLogFlushTimeout());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideStateRetrievalMaxNonProgressingLogWritesPropIfExplicitlySet()
            throws Exception {
      final int expectedStateRetrievalMaxNonProgressingLogWrites = 123456;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest
               .setStateRetrievalMaxNonProgressingLogWrites(expectedStateRetrievalMaxNonProgressingLogWrites);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set StateRetrievalMaxNonProgressingLogWrites property. However, it didn't.",
                        expectedStateRetrievalMaxNonProgressingLogWrites,
                        defaultConfiguration.getStateRetrievalMaxNonProgressingLogWrites());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideStateRetrievalInitialRetryWaitTimePropIfExplicitlySet()
            throws Exception {
      final long expectedStateRetrievalInitialRetryWaitTime = 987665L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest
               .setStateRetrievalInitialRetryWaitTime(expectedStateRetrievalInitialRetryWaitTime);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set StateRetrievalInitialRetryWaitTime property. However, it didn't.",
                        expectedStateRetrievalInitialRetryWaitTime,
                        defaultConfiguration.getStateRetrievalInitialRetryWaitTime());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideStateRetrievalRetryWaitTimeIncreaseFactorPropIfExplicitlySet()
            throws Exception {
      final int expectedStateRetrievalRetryWaitTimeIncreaseFactor = 987432;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest
               .setStateRetrievalRetryWaitTimeIncreaseFactor(expectedStateRetrievalRetryWaitTimeIncreaseFactor);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set StateRetrievalRetryWaitTimeIncreaseFactor property. However, it didn't.",
                        expectedStateRetrievalRetryWaitTimeIncreaseFactor,
                        defaultConfiguration.getStateRetrievalRetryWaitTimeIncreaseFactor());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideStateRetrievalNumRetriesPropIfExplicitlySet()
            throws Exception {
      final int expectedStateRetrievalNumRetries = 765123;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setStateRetrievalNumRetries(expectedStateRetrievalNumRetries);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set StateRetrievalNumRetries property. However, it didn't.",
                        expectedStateRetrievalNumRetries,
                        defaultConfiguration.getStateRetrievalNumRetries());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setIsolationLevelClass(java.lang.String)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideIsolationLevelClassPropIfExplicitlySet()
            throws Exception {
      final String expectedIsolationLevelClass = "REPEATABLE_READ";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setIsolationLevelClass(expectedIsolationLevelClass);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set IsolationLevelClass property. However, it didn't.",
                        IsolationLevel.REPEATABLE_READ, defaultConfiguration.getIsolationLevel());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideUseLazyDeserializationPropIfExplicitlySet()
            throws Exception {
      final boolean expectedUseLazyDeserialization = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseLazyDeserialization(expectedUseLazyDeserialization);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set UseLazyDeserialization property. However, it didn't.",
                        expectedUseLazyDeserialization, defaultConfiguration.isStoreAsBinary());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideL1CacheEnabledPropIfExplicitlySet()
            throws Exception {
      final boolean expectedL1CacheEnabled = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setL1CacheEnabled(expectedL1CacheEnabled);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set L1CacheEnabled property. However, it didn't.",
                        expectedL1CacheEnabled, defaultConfiguration.isL1CacheEnabled());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideL1LifespanPropIfExplicitlySet()
            throws Exception {
      final long expectedL1Lifespan = 2300446L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setL1Lifespan(expectedL1Lifespan);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set L1Lifespan property. However, it didn't.",
                        expectedL1Lifespan, defaultConfiguration.getL1Lifespan());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideL1OnRehashPropIfExplicitlySet()
            throws Exception {
      final boolean expectedL1OnRehash = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setL1OnRehash(expectedL1OnRehash);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set L1OnRehash property. However, it didn't.",
                        expectedL1OnRehash, defaultConfiguration.isL1OnRehash());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setConsistentHashClass(java.lang.String)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideConsistentHashClassPropIfExplicitlySet()
            throws Exception {
      final String expectedConsistentHashClass = "expected.consistent.hash.Class";

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setConsistentHashClass(expectedConsistentHashClass);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set ConsistentHashClass property. However, it didn't.",
                        expectedConsistentHashClass, defaultConfiguration.getConsistentHashClass());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideNumOwnersPropIfExplicitlySet()
            throws Exception {
      final int expectedNumOwners = 675443;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setNumOwners(expectedNumOwners);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set NumOwners property. However, it didn't.",
                        expectedNumOwners, defaultConfiguration.getNumOwners());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideRehashEnabledPropIfExplicitlySet()
            throws Exception {
      final boolean expectedRehashEnabled = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setRehashEnabled(expectedRehashEnabled);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set RehashEnabled property. However, it didn't.",
                        expectedRehashEnabled, defaultConfiguration.isRehashEnabled());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideRehashWaitTimePropIfExplicitlySet()
            throws Exception {
      final long expectedRehashWaitTime = 1232778L;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setRehashWaitTime(expectedRehashWaitTime);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set RehashWaitTime property. However, it didn't.",
                        expectedRehashWaitTime, defaultConfiguration.getRehashWaitTime());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideUseAsyncMarshallingPropIfExplicitlySet()
            throws Exception {
      final boolean expectedUseAsyncMarshalling = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setUseAsyncMarshalling(expectedUseAsyncMarshalling);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set UseAsyncMarshalling property. However, it didn't.",
                        expectedUseAsyncMarshalling, defaultConfiguration.isUseAsyncMarshalling());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideIndexingEnabledPropIfExplicitlySet()
            throws Exception {
      final boolean expectedIndexingEnabled = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setIndexingEnabled(expectedIndexingEnabled);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set IndexingEnabled property. However, it didn't.",
                        expectedIndexingEnabled, defaultConfiguration.isIndexingEnabled());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationOverrides#applyOverridesTo(org.infinispan.config.Configuration)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideIndexLocalOnlyPropIfExplicitlySet()
            throws Exception {
      final boolean expectedIndexLocalOnly = true;

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setIndexLocalOnly(expectedIndexLocalOnly);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set IndexLocalOnly property. However, it didn't.",
                        expectedIndexLocalOnly, defaultConfiguration.isIndexLocalOnly());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanConfigurationFactoryBean#setCustomInterceptors(java.util.List)}
    * .
    */
   @Test
   public final void configurationOverridesShouldOverrideCustomInterceptorsPropIfExplicitlySet()
            throws Exception {
      final CustomInterceptorConfig customInterceptor = new CustomInterceptorConfig();
      final List<CustomInterceptorConfig> expectedCustomInterceptors = Arrays
               .asList(customInterceptor);

      final ConfigurationOverrides objectUnderTest = new ConfigurationOverrides();
      objectUnderTest.setCustomInterceptors(expectedCustomInterceptors);
      final Configuration defaultConfiguration = new Configuration();
      objectUnderTest.applyOverridesTo(defaultConfiguration);

      AssertJUnit
               .assertEquals(
                        "ConfigurationOverrides should have overridden default value with explicitly set CustomInterceptors property. However, it didn't.",
                        expectedCustomInterceptors, defaultConfiguration.getCustomInterceptors());
   }
}
