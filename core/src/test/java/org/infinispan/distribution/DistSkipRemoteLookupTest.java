package org.infinispan.distribution;

import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSkipRemoteLookupTest")
public class DistSkipRemoteLookupTest extends BaseDistFunctionalTest {
   public void testSkipLookupOnGet() {
      MagicKey k1 = new MagicKey(c1);
      c1.put(k1, "value");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert c4.getAdvancedCache().get(k1, SKIP_REMOTE_LOOKUP) == null;

      assertOwnershipAndNonOwnership(k1);
   }

   public void testCorrectFunctionalityOnConditionalWrite() {
      MagicKey k1 = new MagicKey(c1);
      c1.put(k1, "value");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert c4.getAdvancedCache().putIfAbsent(k1, "new_val", SKIP_REMOTE_LOOKUP) == null;

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsInL1(c4, k1);
   }

   public void testCorrectFunctionalityOnUnconditionalWrite() {
      MagicKey k1 = new MagicKey(c1);
      c1.put(k1, "value");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert c4.getAdvancedCache().put(k1, "new_val", SKIP_REMOTE_LOOKUP) == null;
      assert c3.get(k1).equals("new_val");
      assertOnAllCachesAndOwnership(k1, "new_val");
   }
}
