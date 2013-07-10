package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.CacheStore;
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
