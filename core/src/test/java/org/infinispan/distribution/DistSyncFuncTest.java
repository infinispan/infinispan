package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ObjectDuplicator;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@Test(groups = "functional", testName = "distribution.DistSyncFuncTest", enabled = false)
public class DistSyncFuncTest extends BaseDistFunctionalTest {

   public DistSyncFuncTest() {
      sync = true;
      tx = false;
      testRetVals = true;
   }

   public void testBasicDistribution() {
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      getOwners("k1")[0].put("k1", "value");

      asyncWait("k1", PutKeyValueCommand.class, getNonOwners("k1"));

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
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      Object retval = nonOwner.put("k1", "value2");
      asyncWait("k1", PutKeyValueCommand.class, getSecondNonOwner("k1"));

      if (testRetVals) assert "value".equals(retval);
      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testPutIfAbsentFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");

      if (testRetVals) assert "value".equals(retval);

      assertOnAllCachesAndOwnership("k1", "value");

      c1.clear();
      asyncWait(null, ClearCommand.class);

      retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");
      asyncWait("k1", PutKeyValueCommand.class, getSecondNonOwner("k1"));
      if (testRetVals) assert null == retval;

      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testRemoveFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").remove("k1");
      asyncWait("k1", RemoveCommand.class, getSecondNonOwner("k1"));
      if (testRetVals) assert "value".equals(retval);

      assertOnAllCachesAndOwnership("k1", null);
   }

   public void testConditionalRemoveFromNonOwner() {
      initAndTest();
      boolean retval = getFirstNonOwner("k1").remove("k1", "value2");
      if (testRetVals) assert !retval : "Should not have removed entry";

      assertOnAllCachesAndOwnership("k1", "value");

      assert caches.get(1).get("k1").equals("value");

      Cache<Object, String> owner = getFirstNonOwner("k1");

      retval = owner.remove("k1", "value");
      asyncWait("k1", RemoveCommand.class, getSecondNonOwner("k1"));
      if (testRetVals) assert retval : "Should have removed entry";

      assert caches.get(1).get("k1") == null : "expected null but received " + caches.get(1).get("k1");
      assertOnAllCachesAndOwnership("k1", null);
   }

   public void testReplaceFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").replace("k1", "value2");
      if (testRetVals) assert "value".equals(retval);

      asyncWait("k1", ReplaceCommand.class, getSecondNonOwner("k1"));

      assertOnAllCachesAndOwnership("k1", "value2");

      c1.clear();
      asyncWait(null, ClearCommand.class);

      retval = getFirstNonOwner("k1").replace("k1", "value2");
      if (testRetVals) assert retval == null;

      assertOnAllCachesAndOwnership("k1", null);
   }

   public void testConditionalReplaceFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");
      boolean retval = nonOwner.replace("k1", "valueX", "value2");
      if (testRetVals) assert !retval : "Should not have replaced";

      assertOnAllCachesAndOwnership("k1", "value");

      assert !nonOwner.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class).isLocal("k1");
      retval = nonOwner.replace("k1", "value", "value2");
      asyncWait("k1", ReplaceCommand.class, getSecondNonOwner("k1"));
      if (testRetVals) assert retval : "Should have replaced";

      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testClear() {
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      for (int i = 0; i < 10; i++) {
         getOwners("k" + i)[0].put("k" + i, "value" + i);
         asyncWait("k" + i, PutKeyValueCommand.class, getNonOwners("k" + i));
      }

      // this will fill up L1 as well
      for (int i = 0; i < 10; i++) assertOnAllCachesAndOwnership("k" + i, "value" + i);

      for (Cache<Object, String> c : caches) assert !c.isEmpty();

      c1.clear();
      asyncWait(null, ClearCommand.class);

      for (Cache<Object, String> c : caches) assert c.isEmpty();
   }

   public void testKeyValueEntryCollections() {
      c1.put("1", "one");
      asyncWait("1", PutKeyValueCommand.class, getNonOwnersExcludingSelf("1", addressOf(c1)));
      c2.put("2", "two");
      asyncWait("2", PutKeyValueCommand.class, getNonOwnersExcludingSelf("2", addressOf(c2)));
      c3.put("3", "three");
      asyncWait("3", PutKeyValueCommand.class, getNonOwnersExcludingSelf("3", addressOf(c3)));
      c4.put("4", "four");
      asyncWait("4", PutKeyValueCommand.class, getNonOwnersExcludingSelf("4", addressOf(c4)));

      for (Cache c : caches) {
         Set expKeys = TestingUtil.getInternalKeys(c);
         Collection expValues = TestingUtil.getInternalValues(c);

         Set expKeyEntries = ObjectDuplicator.duplicateSet(expKeys);
         Collection expValueEntries = ObjectDuplicator.duplicateCollection(expValues);

         Set keys = c.keySet();
         for (Object key : keys) assert expKeys.remove(key);
         assert expKeys.isEmpty() : "Did not see keys " + expKeys + " in iterator!";

         Collection values = c.values();
         for (Object value : values) assert expValues.remove(value);
         assert expValues.isEmpty() : "Did not see keys " + expValues + " in iterator!";

         Set<Map.Entry> entries = c.entrySet();
         for (Map.Entry entry : entries) {
            assert expKeyEntries.remove(entry.getKey());
            assert expValueEntries.remove(entry.getValue());
         }
         assert expKeyEntries.isEmpty() : "Did not see keys " + expKeyEntries + " in iterator!";
         assert expValueEntries.isEmpty() : "Did not see keys " + expValueEntries + " in iterator!";
      }
   }
}
