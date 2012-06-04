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

package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Distributed, transactional, shared cache store tests.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "distribution.DistSyncTxCacheStoreSharedTest")
public class DistSyncTxCacheStoreSharedTest extends BaseDistCacheStoreTest {

   public DistSyncTxCacheStoreSharedTest() {
      sync = true;
      tx = true;
      testRetVals = true;
      shared = true;
      INIT_CLUSTER_SIZE = 2;
      numOwners = 1;
   }

   public void testPutFromNonOwner() throws Exception {
      Cache<Object, String> cacheX = getFirstNonOwner("key1");
      CacheStore storeX = TestingUtil.extractComponent(
            cacheX, CacheLoaderManager.class).getCacheStore();
      cacheX.put("key1", "v1");
      assertEquals("v1", cacheX.get("key1"));
      assertNotNull(storeX.load("key1"));
      assertEquals("v1", storeX.load("key1").getValue());
   }

}
