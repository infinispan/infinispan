package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.Cache;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Distributed, transactional, shared cache store tests.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "distribution.DistSyncTxStoreSharedTest")
public class DistSyncTxStoreSharedTest extends BaseDistStoreTest {

   public DistSyncTxStoreSharedTest() {
      transactional = true;
      testRetVals = true;
      shared = true;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new DistSyncTxStoreSharedTest().segmented(true),
            new DistSyncTxStoreSharedTest().segmented(false),
      };
   }

   public void testPutFromNonOwner() throws Exception {
      Cache<Object, String> cacheX = getFirstNonOwner("key1");
      CacheLoader storeX = TestingUtil.getFirstLoader(cacheX);
      cacheX.put("key1", "v1");
      assertEquals("v1", cacheX.get("key1"));
      assertNotNull(storeX.loadEntry("key1"));
      assertEquals("v1", storeX.loadEntry("key1").getValue());
   }

}
