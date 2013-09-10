package org.infinispan.distribution;

import org.infinispan.Cache;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertFalse;

/**
 * Test distribution when L1 is disabled and return values are needed.
 *
 * @author Galder Zamarreño
 * @author Manik Surtani
 * @since 5.0
 */
@Test(groups = "functional", testName = "distribution.DisabledL1WithRetValsTest")
public class DisabledL1WithRetValsTest extends BaseDistFunctionalTest<Object, String> {

   public DisabledL1WithRetValsTest() {
      l1CacheEnabled = false;
      testRetVals = true;
      numOwners = 1;
      INIT_CLUSTER_SIZE = 2;
   }

   public void testReplaceFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      Object retval = nonOwner.replace("k1", "value2");

      assert "value".equals(retval);
      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testConditionalReplaceFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      boolean success = nonOwner.replace("k1", "blah", "value2");
      assert !success;

      assertOnAllCachesAndOwnership("k1", "value");

      success = nonOwner.replace("k1", "value", "value2");
      assert success;

      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testPutFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      Object retval = nonOwner.put("k1", "value2");

      assert "value".equals(retval);
      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testRemoveFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      Object retval = nonOwner.remove("k1");

      assert "value".equals(retval);
      assertRemovedOnAllCaches("k1");
   }

   public void testConditionalRemoveFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      boolean removed = nonOwner.remove("k1", "blah");
      assert !removed;

      assertOnAllCachesAndOwnership("k1", "value");

      removed = nonOwner.remove("k1", "value");
      assert removed;

      assertRemovedOnAllCaches("k1");
   }

   public void testPutIfAbsentFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");

      assert "value".equals(retval);

      assertOnAllCachesAndOwnership("k1", "value");

      c1.clear();

      assertFalse(c1.getAdvancedCache().getLockManager().isLocked("k1"));
      assertFalse(c2.getAdvancedCache().getLockManager().isLocked("k1"));

      retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");
      assert null == retval;

      assertOnAllCachesAndOwnership("k1", "value2");
   }
}
