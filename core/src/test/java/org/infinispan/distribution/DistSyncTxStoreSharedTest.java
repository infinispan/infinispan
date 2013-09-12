package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.persistence.spi.CacheLoader;
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
@Test(groups = "functional", testName = "distribution.DistSyncTxStoreSharedTest")
public class DistSyncTxStoreSharedTest extends BaseDistStoreTest {

   public DistSyncTxStoreSharedTest() {
      sync = true;
      tx = true;
      testRetVals = true;
      shared = true;
      INIT_CLUSTER_SIZE = 2;
      numOwners = 1;
   }

   public void testPutFromNonOwner() throws Exception {
      Cache<Object, String> cacheX = getFirstNonOwner("key1");
      CacheLoader storeX = TestingUtil.getFirstLoader(cacheX);
      cacheX.put("key1", "v1");
      assertEquals("v1", cacheX.get("key1"));
      assertNotNull(storeX.load("key1"));
      assertEquals("v1", storeX.load("key1").getValue());
   }

}
