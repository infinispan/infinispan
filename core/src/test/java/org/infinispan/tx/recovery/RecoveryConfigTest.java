/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.tx.recovery;

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryInfoKey;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.testng.annotations.Test;

import static org.infinispan.configuration.cache.RecoveryConfiguration.DEFAULT_RECOVERY_INFO_CACHE;
import static org.infinispan.tx.recovery.RecoveryTestUtil.rm;
import static org.testng.Assert.*;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.RecoveryConfigTest")
public class RecoveryConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml("configs/recovery-enabled-config.xml");
   }

   @Test(expectedExceptions = ConfigurationException.class)
   public void testRecoveryAndAsyncCaches() {
      //Note: this configuration uses Xa Enlistment (see configs/recovery-enabled-config.xml).
      Configuration defaultConfig = cacheManager.getDefaultCacheConfiguration();
      ConfigurationBuilder builder = new ConfigurationBuilder().read(defaultConfig);
      builder.clustering().cacheMode(CacheMode.REPL_ASYNC);
      builder.transaction().recovery().enable();
      //it should throw an exception when try to build this configuration.
      builder.build();
   }

   @Test(expectedExceptions = ConfigurationException.class)
   public void testRecoveryAndAsyncCommitPhaseCaches() {
      //Note: this configuration uses Xa Enlistment (see configs/recovery-enabled-config.xml).
      Configuration defaultConfig = cacheManager.getDefaultCacheConfiguration();
      ConfigurationBuilder builder = new ConfigurationBuilder().read(defaultConfig);
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      builder.transaction().syncCommitPhase(false).recovery().enable();
      //it should throw an exception when try to build this configuration.
      builder.build();
   }

   public void testRecoveryWithCacheConfigured() {
      Configuration withRecoveryAndCache = cacheManager.getCache("withRecoveryAndCache").getCacheConfiguration();
      assertTrue(withRecoveryAndCache.transaction().recovery().enabled(), "Recovery is supposed to be enabled.");
      assertEquals(withRecoveryAndCache.transaction().recovery().recoveryInfoCacheName(), "noRecovery", "Wrong recovery cache name.");
      RecoveryManagerImpl recoveryManager = rm(cacheManager.getCache("withRecoveryAndCache"));
      assertNotNull(recoveryManager, "RecoveryManager should be *not* null when recovery is enabled.");
      Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction> preparedTransactions = (Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction>) recoveryManager.getInDoubtTransactionsMap();
      assertEquals(preparedTransactions.getName(), "noRecovery", "Wrong recovery cache name.");
   }

   public void testRecoveryWithDefaultCache() {
      Configuration recoveryDefaultCache = cacheManager.getCache("withRecoveryDefaultCache").getCacheConfiguration();
      assertTrue(recoveryDefaultCache.transaction().recovery().enabled(), "Recovery is supposed to be enabled.");
      assertEquals(recoveryDefaultCache.transaction().recovery().recoveryInfoCacheName(), DEFAULT_RECOVERY_INFO_CACHE, "Wrong recovery cache name.");
      RecoveryManagerImpl recoveryManager = rm(cacheManager.getCache("withRecoveryDefaultCache"));
      assertNotNull(recoveryManager, "RecoveryManager should be *not* null when recovery is enabled.");
      Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction> preparedTransactions = (Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction>) recoveryManager.getInDoubtTransactionsMap();
      assertEquals(preparedTransactions.getName(), DEFAULT_RECOVERY_INFO_CACHE, "Wrong recovery cache name.");
   }

   public void testNoRecovery() {
      Configuration noRecovery = cacheManager.getCache("noRecovery").getCacheConfiguration();
      assertFalse(noRecovery.transaction().recovery().enabled(), "Recovery is supposed to be disabled");
      assertNull(rm(cacheManager.getCache("noRecovery")), "RecoveryManager should be null when recovery is disabled");
      assertEquals(noRecovery.transaction().recovery().recoveryInfoCacheName(), "someName", "Wrong recovery cache name.");
   }
}
