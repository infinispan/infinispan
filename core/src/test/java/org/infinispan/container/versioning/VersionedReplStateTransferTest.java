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

package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

@Test(testName = "container.versioning.VersionedReplStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class VersionedReplStateTransferTest extends MultipleCacheManagersTest {

   ConfigurationBuilder builder;

   @Override
   protected void createCacheManagers() throws Throwable {
      builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      builder
            .clustering()
               .cacheMode(CacheMode.REPL_SYNC)
            .versioning()
               .enable()
               .scheme(VersioningScheme.SIMPLE)
            .locking()
               .isolationLevel(IsolationLevel.REPEATABLE_READ)
               .writeSkewCheck(true)
            .transaction()
               .lockingMode(LockingMode.OPTIMISTIC)
               .syncCommitPhase(true);

      createCluster(builder, 2);
      waitForClusterToForm();
   }

   public void testStateTransfer() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      cache0.put("hello", "world");

      assert "world".equals(cache0.get("hello"));
      assert "world".equals(cache1.get("hello"));

      tm(1).begin();
      assert "world".equals(cache1.get("hello"));
      Transaction t = tm(1).suspend();

      // create a cache2
      addClusterEnabledCacheManager(builder);
      Cache<Object, Object> cache2 = cache(2);

      assert "world".equals(cache2.get("hello"));

      cacheManagers.get(0).stop();

      // Cause a write skew
      cache2.put("hello", "new world");

      tm(1).resume(t);
      cache1.put("hello", "world2");

      try {
         tm(1).commit();
         assert false : "Should fail";
      } catch (RollbackException expected) {
         // Expected
      }

      assert "new world".equals(cache(1).get("hello"));
      assert "new world".equals(cache(2).get("hello"));
   }
}
