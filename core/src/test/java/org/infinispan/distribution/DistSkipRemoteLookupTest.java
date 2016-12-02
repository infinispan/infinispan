package org.infinispan.distribution;

import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.ExecutionException;

import org.infinispan.context.Flag;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSkipRemoteLookupTest")
public class DistSkipRemoteLookupTest extends BaseDistFunctionalTest<Object, String> {

   @Override
   public Object[] factory() {
      return new Object[] {
         new DistSkipRemoteLookupTest(),
         new DistSkipRemoteLookupTest().l1(false),
      };
   }

   public DistSkipRemoteLookupTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testSkipLookupOnGet() {
      MagicKey k1 = new MagicKey(c1, c2);
      c1.put(k1, "value");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert c4.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k1) == null;

      assertOwnershipAndNonOwnership(k1, false);
   }

   @Test(enabled = false, description = "does it make sense to have skip_remote_lookup with conditional commands?")
   public void testCorrectFunctionalityOnConditionalWrite() {
      MagicKey k1 = new MagicKey(c1, c2);
      c1.put(k1, "value");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert c4.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).putIfAbsent(k1, "new_val") == null;

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      if (l1CacheEnabled) assertIsNotInL1(c4, k1);
   }

   public void testCorrectFunctionalityOnUnconditionalWrite() {
      MagicKey k1 = new MagicKey(c1, c2);
      c1.put(k1, "value");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert c4.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).put(k1, "new_val") == null;
      assert c3.get(k1).equals("new_val");
      assertOnAllCachesAndOwnership(k1, "new_val");
   }

   @Test
   public void testSkipLookupOnRemove() {
      MagicKey k1 = new MagicKey(c1, c2);
      final String value = "SomethingToSayHere";

      assert null == c1.put(k1, value);
      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert value.equals(c1.get(k1));
      assert value.equals(c1.remove(k1));
      assert null == c1.put(k1, value);

      assertIsNotInL1(c3, k1);
      assert value.equals(c3.remove(k1));
      assert null == c1.put(k1, value);

      assert null == c4.getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).remove(k1);
   }

   @Test
   public void testSkipLookupOnAsyncRemove() throws InterruptedException, ExecutionException {
      MagicKey k1 = new MagicKey(c1, c2);
      final String value = "SomethingToSayHere-async";

      assert null == c1.put(k1, value);
      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsNotInL1(c3, k1);
      assertIsNotInL1(c4, k1);

      assert value.equals(c1.get(k1));
      assert value.equals(c1.remove(k1));
      assert null == c1.put(k1, value);

      assertIsNotInL1(c3, k1);
      log.trace("here it is");
      assertEquals(value, c3.remove(k1));
      assert null == c1.put(k1, value);

      assert null == c4.getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).removeAsync(k1).get();
   }

}
