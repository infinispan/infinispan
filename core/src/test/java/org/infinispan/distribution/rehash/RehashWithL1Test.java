/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.distribution.rehash;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Tests rehashing with distributed caches with L1 enabled.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "distribution.rehash.RehashWithL1Test")
public class RehashWithL1Test extends MultipleCacheManagersTest {

   ConfigurationBuilder builder;

   @Override
   protected void createCacheManagers() throws Throwable {
      builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      // Enable rehashing explicitly
      builder.clustering().stateTransfer().fetchInMemoryState(true)
            .hash().l1().enable();
      createClusteredCaches(2, builder);
   }

   public void testPutWithRehashAndCacheClear() throws Exception {
      Future<Void> node3Join = null;
      int opCount = 100;
      for (int i = 0; i < opCount; i++) {
         cache(0).put("k" + i, "some data");
         if (i == (opCount / 2)) {
            node3Join = fork(new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  EmbeddedCacheManager cm = addClusterEnabledCacheManager(builder,
                        new TransportFlags().withMerge(true));
                  cm.getCache();
                  return null;
               }
            });
         }
         Thread.sleep(10);
      }

      if (node3Join == null) throw new Exception("Node 3 not joined");
      node3Join.get();

      for (int i = 0; i < opCount; i++) {
         cache(0).remove("k" + i);
         Thread.sleep(10);
      }

      for (int i = 0; i < opCount; i++) {
         String key = "k" + i;
         assertFalse(cache(0).containsKey(key));
         assertFalse("Key: " + key + " is present in cache at " + cache(1),
               cache(1).containsKey(key));
         assertFalse("Key: " + key + " is present in cache at " + cache(2),
               cache(2).containsKey(key));
      }

      assertEquals(0, cache(0).size());
      assertEquals(0, cache(1).size());
      assertEquals(0, cache(2).size());
   }

}
