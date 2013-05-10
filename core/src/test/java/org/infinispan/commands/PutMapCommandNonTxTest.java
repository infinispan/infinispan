/*
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.commands;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

@Test(groups = "functional", testName = "commands.PutMapCommandNonTxTest")
@CleanupAfterMethod
public class PutMapCommandNonTxTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      dcc.locking().supportsConcurrentUpdates(true);
      dcc.clustering().hash().numOwners(3).l1().disable();

      createCluster(dcc, 3);
      waitForClusterToForm();
   }

   public void testPutMapCommandSyncOnPrimaryOwner() throws Exception {
      testPutMapCommand(true, true);
   }

   public void testPutMapCommandAsyncOnPrimaryOwner() throws Exception {
      testPutMapCommand(false, true);
   }

   public void testPutMapCommandSyncOnBackupOwner() throws Exception {
      testPutMapCommand(true, false);
   }

   public void testPutMapCommandAsyncOnBackupOwner() throws Exception {
      testPutMapCommand(false, false);
   }

   private void testPutMapCommand(boolean sync, boolean putOnPrimary) throws Exception {
      MagicKey key = new MagicKey("key", cache(0));

      if (sync) {
         cache(putOnPrimary ? 0 : 1).putAll(Collections.singletonMap(key, "value"));
      } else {
         Future<Void> f = cache(putOnPrimary ? 0 : 1).putAllAsync(Collections.singletonMap(key, "value"));
         assertNotNull(f);
         assertNull(f.get());
         assertTrue(f.isDone());
         assertFalse(f.isCancelled());
      }

      assertEquals("value", cache(0).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
      assertEquals("value", cache(1).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
      assertEquals("value", cache(2).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
   }
}