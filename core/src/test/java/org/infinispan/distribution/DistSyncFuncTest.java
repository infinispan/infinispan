package org.infinispan.distribution;

import org.infinispan.Cache;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSyncFuncTest", enabled = false)
public class DistSyncFuncTest extends BaseDistFunctionalTest {

   public DistSyncFuncTest() {
      sync = true;
      tx = false;
   }

   public void testBasicDistribution() {
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      c1.put("k1", "value");

      asyncWait();

      for (Cache<Object, String> c : caches) {
         if (isOwner(c, "k1")) {
            assertIsInContainerImmortal(c, "k1");
         } else {
            assertIsNotInL1(c, "k1");
         }
      }

      // should be available everywhere!
      assertOnAllCachesAndOwnership("k1", "value");

      // and should now be in L1

      for (Cache<Object, String> c : caches) {
         if (isOwner(c, "k1")) {
            assertIsInContainerImmortal(c, "k1");
         } else {
            assertIsInL1(c, "k1");
         }
      }
   }

   public void testPutFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").put("k1", "value2");
      asyncWait();
      if (sync) assert "value".equals(retval);
      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testPutIfAbsentFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");
      asyncWait();
      if (sync) assert "value".equals(retval);

      assertOnAllCachesAndOwnership("k1", "value");

      c1.clear();
      asyncWait();

      retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");
      asyncWait();
      if (sync) assert null == retval;

      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testRemoveFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").remove("k1");
      asyncWait();
      if (sync) assert "value".equals(retval);

      assertOnAllCachesAndOwnership("k1", null);
   }

   public void testConditionalRemoveFromNonOwner() {
      initAndTest();
      boolean retval = getFirstNonOwner("k1").remove("k1", "value2");
      asyncWait();
      if (sync) assert !retval : "Should not have removed entry";

      assertOnAllCachesAndOwnership("k1", "value");

      retval = getFirstNonOwner("k1").remove("k1", "value");
      asyncWait();
      if (sync) assert retval : "Should have removed entry";

      assertOnAllCachesAndOwnership("k1", null);
   }

   public void testReplaceFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").replace("k1", "value2");
      asyncWait();
      if (sync) assert "value".equals(retval);

      assertOnAllCachesAndOwnership("k1", "value2");

      c1.clear();
      asyncWait();

      retval = getFirstNonOwner("k1").replace("k1", "value2");
      asyncWait();
      if (sync) assert retval == null;

      assertOnAllCachesAndOwnership("k1", null);
   }

   public void testConditionalReplaceFromNonOwner() {
      initAndTest();
      boolean retval = getFirstNonOwner("k1").replace("k1", "valueX", "value2");
      asyncWait();
      if (sync) assert !retval : "Should not have replaced";

      assertOnAllCachesAndOwnership("k1", "value");

      c1.clear();
      asyncWait();

      retval = getFirstNonOwner("k1").replace("k1", "value", "value2");
      asyncWait();
      if (sync) assert retval : "Should have replaced";

      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testClear() {
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      for (int i = 0; i < 10; i++) {
         c1.put("k" + i, "value" + i);
         asyncWait();
      }

      // this will fill up L1 as well
      for (int i = 0; i < 10; i++) assertOnAllCachesAndOwnership("k" + i, "value" + i);

      for (Cache<Object, String> c : caches) assert !c.isEmpty();

      c1.clear();
      asyncWait();

      for (Cache<Object, String> c : caches) assert c.isEmpty();
   }
}
