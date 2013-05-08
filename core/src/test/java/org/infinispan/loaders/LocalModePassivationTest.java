/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test if keys are properly passivated and reloaded in local mode (to ensure fix for ISPN-2712 did no break local mode).
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "loaders.LocalModePassivationTest")
@CleanupAfterMethod
public class LocalModePassivationTest extends SingleCacheManagerTest {

   private File cacheStoreDir;

   private final boolean passivationEnabled;

   protected LocalModePassivationTest() {
      passivationEnabled = true;
   }

   protected LocalModePassivationTest(boolean passivationEnabled) {
      this.passivationEnabled = passivationEnabled;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheStoreDir = new File(TestingUtil.tmpDirectory(this));
      TestingUtil.recursiveFileRemove(cacheStoreDir);

      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true, true);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .eviction().maxEntries(1000).strategy(EvictionStrategy.LIRS)
            .locking().lockAcquisitionTimeout(20000)
            .concurrencyLevel(5000)
            .useLockStriping(false).writeSkewCheck(false).isolationLevel(IsolationLevel.READ_COMMITTED)
            .dataContainer().storeAsBinary()
            .loaders().passivation(passivationEnabled).preload(false).addFileCacheStore().location(cacheStoreDir.getAbsolutePath())
            .fetchPersistentState(true)
            .purgerThreads(3)
            .purgeSynchronously(true)
            .ignoreModifications(false)
            .purgeOnStartup(false);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(cacheStoreDir);
   }

   public void testStoreAndLoad() throws Exception {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache().put(i, i);
      }

      int keysInDataContainer = cache().getAdvancedCache().getDataContainer().keySet().size();

      assertTrue(keysInDataContainer != numKeys); // some keys got evicted

      CacheLoaderManager cml = cache().getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      int keysInCacheStore = cml.getCacheLoader().loadAll().size();

      if (passivationEnabled) {
         assertEquals(numKeys, keysInDataContainer + keysInCacheStore);
      } else {
         assertEquals(numKeys, keysInCacheStore);
      }

      // check if keys survive restart
      cache().stop();
      cache().start();

      cml = cache().getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      assertEquals(numKeys, cml.getCacheLoader().loadAll().size());

      for (int i = 0; i < numKeys; i++) {
         assertEquals(i, cache().get(i));
      }
   }
}
