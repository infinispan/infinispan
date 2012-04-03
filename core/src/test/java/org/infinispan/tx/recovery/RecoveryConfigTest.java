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
import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryInfoKey;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.testng.annotations.Test;

import static org.infinispan.tx.recovery.RecoveryTestUtil.rm;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.RecoveryConfigTest", enabled = false, description = "Disabled due to instability - see ISPN-1123")
public class RecoveryConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml("configs/recovery-enabled-config.xml");
   }

   public void testRecoveryAndAsyncCaches() {
      try {
         cacheManager.getCache("withRecoveryAndAsync");
         assert false;
      } catch (Exception e) {
         //expected
      }
   }

   public void testRecoveryWithCacheConfigured() {
      Configuration withRecoveryAndCache = cacheManager.getCache("withRecoveryAndCache").getConfiguration();
      assert withRecoveryAndCache.isTransactionRecoveryEnabled();
      assertEquals(withRecoveryAndCache.getTransactionRecoveryCacheName(), "noRecovery");
      RecoveryManagerImpl recoveryManager = rm(cacheManager.getCache("withRecoveryAndCache"));
      Cache<RecoveryInfoKey,RecoveryAwareRemoteTransaction> preparedTransactions = (Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction>) recoveryManager.getInDoubtTransactionsMap();
      assertEquals(preparedTransactions.getName(), "noRecovery");
   }

   public void testRecoveryWithDefaultCache() {
      Configuration recoveryDefaultCache = cacheManager.getCache("withRecoveryDefaultCache").getConfiguration();
      assert recoveryDefaultCache.isTransactionRecoveryEnabled();
      assertEquals(recoveryDefaultCache.getTransactionRecoveryCacheName(), Configuration.RecoveryType.DEFAULT_RECOVERY_INFO_CACHE);
      RecoveryManagerImpl recoveryManager = rm(cacheManager.getCache("withRecoveryDefaultCache"));
      Cache<RecoveryInfoKey,RecoveryAwareRemoteTransaction> preparedTransactions = (Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction>) recoveryManager.getInDoubtTransactionsMap();
      assertEquals(preparedTransactions.getName(), Configuration.RecoveryType.DEFAULT_RECOVERY_INFO_CACHE);
   }

   public void testNoRecovery() {
      Configuration noRecovery = cacheManager.getCache("noRecovery").getConfiguration();
      assert !noRecovery.isTransactionRecoveryEnabled();
      assertEquals("someName", noRecovery.getTransactionRecoveryCacheName());
   }
}
