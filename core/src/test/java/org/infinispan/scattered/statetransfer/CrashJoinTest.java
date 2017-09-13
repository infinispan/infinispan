package org.infinispan.scattered.statetransfer;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

import java.util.List;


/**
 * Basic node/coord crash and node join scenarios.
 */
@Test(groups = "functional", testName = "scattered.statetransfer.CrashJoinTest")
@CleanupAfterMethod
public class CrashJoinTest extends AbstractStateTransferTest {
   private DISCARD d1, d2, d3;

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();

      d1 = TestingUtil.getDiscardForCache(c1);
      d2 = TestingUtil.getDiscardForCache(c2);
      d3 = TestingUtil.getDiscardForCache(c3);
   }

   public void testNodeCrash() {
      List<MagicKey> keys = init();

      assertFalse(c2.getCacheManager().isCoordinator());
      d2.setDiscardAll(true);
      TestingUtil.blockUntilViewsReceived(30000, false, c1, c3);
      TestingUtil.waitForNoRebalance(c1, c3);

      checkValuesInDC(keys, c1, c3);
   }

   public void testCoordCrash() {
      List<MagicKey> keys = init();

      assertTrue(c1.getCacheManager().isCoordinator());
      d1.setDiscardAll(true);
      TestingUtil.blockUntilViewsReceived(30000, false, c2, c3);
      TestingUtil.waitForNoRebalance(c2, c3);

      checkValuesInDC(keys, c2, c3);
   }

   public void testNodeJoin() throws Exception {
      List<MagicKey> keys = init();
      Cache c4 = addClusterEnabledCacheManager(defaultConfig, TRANSPORT_FLAGS).getCache(CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, false, c1, c2, c3, c4);
      TestingUtil.waitForNoRebalance(c1, c2, c3, c4);

      checkValuesInCache(keys, c1, c2, c3, c4);
   }
}
