package org.infinispan.distribution;

import org.infinispan.test.AbstractCacheTest;
import org.testng.annotations.Test;

import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "distribution.DistSkipRemoteLookupBatchingTest")
public class DistSkipRemoteLookupBatchingTest extends BaseDistFunctionalTest<Object, String> {

   public DistSkipRemoteLookupBatchingTest() {
      cleanup = AbstractCacheTest.CleanupPhase.AFTER_METHOD;
      batchingEnabled = true;
      tx = true;
   }

   public void testSkipLookupOnGetWhileBatching() {
      MagicKey k1 = new MagicKey(c1, c2);
      c1.put(k1, "batchingMagicValue-h1");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      c4.startBatch();
      assert c4.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k1) == null;
      c4.endBatch(true);

      assertOwnershipAndNonOwnership(k1, false);
   }
}
