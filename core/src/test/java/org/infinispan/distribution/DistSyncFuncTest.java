package org.infinispan.distribution;

import static org.infinispan.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.ObjectDuplicator;
import org.infinispan.context.Flag;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableFunction;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "distribution.DistSyncFuncTest")
public class DistSyncFuncTest extends BaseDistFunctionalTest<Object, String> {

   public DistSyncFuncTest() {
      testRetVals = true;
   }

   public void testLocationConsensus() {
      String[] keys = new String[100];
      Random r = new Random();
      for (int i = 0; i < 100; i++) keys[i] = Integer.toHexString(r.nextInt());

      for (String key : keys) {
         List<Address> owners = new ArrayList<>();
         for (Cache<Object, String> c : caches) {
            boolean isOwner = isOwner(c, key);
            if (isOwner) owners.add(addressOf(c));
            boolean secondCheck = getCacheTopology(c).getWriteOwners(key).contains(addressOf(c));
            assertTrue("Second check failed for key " + key + " on cache " + addressOf(c) + " isO = " + isOwner + " sC = " + secondCheck, isOwner == secondCheck);
         }
         // check consensus
         assertOwnershipConsensus(key);
         assertEquals("Expected " + numOwners + " owners for key " + key + " but was " + owners, numOwners, owners.size());
      }
   }

   private void assertOwnershipConsensus(String key) {
      List l1 = getCacheTopology(c1).getDistribution(key).writeOwners();
      List l2 = getCacheTopology(c2).getDistribution(key).writeOwners();
      List l3 = getCacheTopology(c3).getDistribution(key).writeOwners();
      List l4 = getCacheTopology(c4).getDistribution(key).writeOwners();

      assertEquals("L1 "+l1+" and L2 "+l2+" don't agree.", l1, l2);
      assertEquals("L2 "+l2+" and L3 "+l3+" don't agree.", l2, l3);
      assertEquals("L3 "+l3+" and L4 "+l4+" don't agree.", l3, l4);

   }

   public void testBasicDistribution() throws Throwable {
      for (Cache<Object, String> c : caches)
         assertTrue(c.isEmpty());

      final Object k1 = getKeyForCache(caches.get(1));
      getOwners(k1)[0].put(k1, "value");

      // No non-owners have requested the key, so no invalidations
      asyncWait(k1, PutKeyValueCommand.class);

      // should be available everywhere!
      assertOnAllCachesAndOwnership(k1, "value");

      // and should now be in L1
      if (l1CacheEnabled) {
         for (Cache<Object, String> c : caches) {
            if (isOwner(c, k1)) {
               assertIsInContainerImmortal(c, k1);
            } else {
               assertIsInL1(c, k1);
            }
         }
      }
   }

   public void testPutFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");

      Object retval = nonOwner.put("k1", "value2");
      asyncWait("k1", PutKeyValueCommand.class, getSecondNonOwner("k1"));

      if (testRetVals) assertEquals("value", retval);
      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testPutIfAbsentFromNonOwner() {
      initAndTest();
      log.trace("Here it begins");
      Object retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");

      if (testRetVals) assertEquals("value", retval);

      assertOnAllCachesAndOwnership("k1", "value");

      c1.clear();
      asyncWait(null, ClearCommand.class);

      retval = getFirstNonOwner("k1").putIfAbsent("k1", "value2");

      eventually(() -> {
         try {
            assertOnAllCachesAndOwnership("k1", "value2");
         } catch (AssertionError e) {
            log.debugf("Assertion failed once", e);
            return false;
         }
         return true;
      });

      if (testRetVals) assertNull(retval);
   }

   public void testRemoveFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").remove("k1");
      asyncWait("k1", RemoveCommand.class, getSecondNonOwner("k1"));
      if (testRetVals) assertEquals("value", retval);

      assertRemovedOnAllCaches("k1");
   }

   public void testConditionalRemoveFromNonOwner() {
      initAndTest();
      log.trace("Here we start");
      boolean retval = getFirstNonOwner("k1").remove("k1", "value2");
      if (testRetVals) assertFalse("Should not have removed entry", retval);

      assertOnAllCachesAndOwnership("k1", "value");

      assertEquals("value", caches.get(1).get("k1"));

      Cache<Object, String> owner = getFirstNonOwner("k1");

      retval = owner.remove("k1", "value");
      asyncWait("k1", RemoveCommand.class, getSecondNonOwner("k1"));
      if (testRetVals) assertTrue("Should have removed entry", retval);

      assertNull("expected null but received " + caches.get(1).get("k1"), caches.get(1).get("k1"));
      assertRemovedOnAllCaches("k1");
   }

   public void testReplaceFromNonOwner() {
      initAndTest();
      Object retval = getFirstNonOwner("k1").replace("k1", "value2");
      if (testRetVals) assertEquals("value", retval);

      asyncWait("k1", ReplaceCommand.class, getSecondNonOwner("k1"));

      assertOnAllCachesAndOwnership("k1", "value2");

      c1.clear();
      asyncWait(null, ClearCommand.class);

      retval = getFirstNonOwner("k1").replace("k1", "value2");
      if (testRetVals) assertNull(retval);

      assertRemovedOnAllCaches("k1");
   }

   public void testConditionalReplaceFromNonOwner() {
      initAndTest();
      Cache<Object, String> nonOwner = getFirstNonOwner("k1");
      boolean retval = nonOwner.replace("k1", "valueX", "value2");
      if (testRetVals) assertFalse("Should not have replaced", retval);

      assertOnAllCachesAndOwnership("k1", "value");

      assertFalse(extractComponent(nonOwner, DistributionManager.class).getCacheTopology().isWriteOwner("k1"));
      retval = nonOwner.replace("k1", "value", "value2");
      asyncWait("k1", ReplaceCommand.class, getSecondNonOwner("k1"));
      if (testRetVals) assertTrue("Should have replaced", retval);

      assertOnAllCachesAndOwnership("k1", "value2");
   }

   public void testClear() throws InterruptedException {
      for (Cache<Object, String> c : caches)
         assertTrue(c.isEmpty());

      for (int i = 0; i < 10; i++) {
         getOwners("k" + i)[0].put("k" + i, "value" + i);
         // There will be no caches to invalidate as this is the first command of the test
         asyncWait("k" + i, PutKeyValueCommand.class);
         assertOnAllCachesAndOwnership("k" + i, "value" + i);
      }

      // this will fill up L1 as well
      for (int i = 0; i < 10; i++) assertOnAllCachesAndOwnership("k" + i, "value" + i);

      for (Cache<Object, String> c : caches)
         assertFalse(c.isEmpty());

      c1.clear();
      asyncWait(null, ClearCommand.class);

      for (Cache<Object, String> c : caches)
         assertTrue(c.isEmpty());
   }

   public void testKeyValueEntryCollections() {
      c1.put("1", "one");
      asyncWait("1", PutKeyValueCommand.class);
      c2.put("2", "two");
      asyncWait("2", PutKeyValueCommand.class);
      c3.put("3", "three");
      asyncWait("3", PutKeyValueCommand.class);
      c4.put("4", "four");
      asyncWait("4", PutKeyValueCommand.class);

      for (Cache c : caches) {
         Set expKeys = TestingUtil.getInternalKeys(c);
         Collection expValues = TestingUtil.getInternalValues(c);

         Set expKeyEntries = ObjectDuplicator.duplicateSet(expKeys);
         Collection expValueEntries = ObjectDuplicator.duplicateCollection(expValues);

         // CACHE_MODE_LOCAL prohibits RPCs and SKIP_OWNERSHIP_CHECKS forces that all entries from DC are read
         AdvancedCache cacheWithIgnoredOwnership = c.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_OWNERSHIP_CHECK);
         Set keys = cacheWithIgnoredOwnership.keySet();
         for (Object key : keys)
            assertTrue(expKeys.remove(key));
         assertTrue("Did not see keys " + expKeys + " in iterator!", expKeys.isEmpty());

         Collection values = cacheWithIgnoredOwnership.values();
         for (Object value : values)
            assertTrue(expValues.remove(value));
         assertTrue("Did not see keys " + expValues + " in iterator!", expValues.isEmpty());

         Set<Map.Entry> entries = cacheWithIgnoredOwnership.entrySet();
         for (Map.Entry entry : entries) {
            assertTrue(expKeyEntries.remove(entry.getKey()));
            assertTrue(expValueEntries.remove(entry.getValue()));
         }
         assertTrue("Did not see keys " + expKeyEntries + " in iterator!", expKeyEntries.isEmpty());
         assertTrue("Did not see keys " + expValueEntries + " in iterator!", expValueEntries.isEmpty());
      }
   }

   public void testLockedStreamSetValue() {
      int size = 5;
      for (int i = 0; i < size; i++) {
         getOwners("k" + i)[0].put("k" + i, "value" + i);
         // There will be no caches to invalidate as this is the first command of the test
         asyncWait("k" + i, PutKeyValueCommand.class);
         assertOnAllCachesAndOwnership("k" + i, "value" + i);
      }

      c1.getAdvancedCache().lockedStream().forEach((c, e) -> e.setValue(e.getValue() + "-changed"));

      for (int i = 0; i < size; i++) {
         int offset = i;
         String key = "k" + i;
         Cache<Object, String>[] caches = getOwners(key);
         for (Cache<Object, String> cache : caches) {
            eventuallyEquals("value" + offset + "-changed",
                  () -> cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).get("k" + offset));
         }
      }
   }

   public void testLockedStreamPutValue() {
      int size = 5;
      for (int i = 0; i < size; i++) {
         getOwners("k" + i)[0].put("k" + i, "value" + i);
         // There will be no caches to invalidate as this is the first command of the test
         asyncWait("k" + i, PutKeyValueCommand.class);
         assertOnAllCachesAndOwnership("k" + i, "value" + i);
      }

      c1.getAdvancedCache().lockedStream().forEach((c, e) -> c.put(e.getKey(), e.getValue() + "-changed"));

      for (int i = 0; i < size; i++) {
         int offset = i;
         String key = "k" + i;
         Cache<Object, String>[] caches = getOwners(key);
         for (Cache<Object, String> cache : caches) {
            eventuallyEquals("value" + offset + "-changed",
                  () -> cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).get("k" + offset));
         }
      }
   }

   public void testComputeFromNonOwner() throws InterruptedException {
      // compute function applied
      initAndTest();
      Object retval = getFirstNonOwner("k1").compute("k1", (k, v) -> "computed_" + k + "_" + v);
      if (testRetVals) assertEquals("computed_k1_value", retval);
      asyncWait("k1", ComputeCommand.class);
      assertOnAllCachesAndOwnership("k1", "computed_k1_value");

      // remove if after compute value is null
      retval = getFirstNonOwner("k1").compute("k1", (v1, v2) -> null);
      asyncWait("k1", ComputeCommand.class);
      if (testRetVals) assertNull(retval);
      assertRemovedOnAllCaches("k1");

      // put computed value if absent
      retval = getFirstNonOwner("notThere").compute("notThere", (k, v) -> "add_" + k);
      eventually(() -> {
         try {
            assertOnAllCachesAndOwnership("notThere", "add_notThere");
         } catch (AssertionError e) {
            log.debugf("Assertion failed once", e);
            return false;
         }
         return true;
      });
      if (testRetVals) assertEquals("add_notThere", retval);

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      SerializableBiFunction<Object, Object, String> mappingToException = (k, v) -> {
         throw computeRaisedException;
      };
      expectException(RemoteException.class, () -> getFirstNonOwner("k1").compute("k1", mappingToException));
   }

   public void testComputeIfPresentFromNonOwner() throws InterruptedException {
      // compute function applied
      initAndTest();
      Object retval = getFirstNonOwner("k1").computeIfPresent("k1", (k, v) -> "computed_" + k + "_" + v);
      if (testRetVals) assertEquals("computed_k1_value", retval);
      asyncWait("k1", ComputeCommand.class);
      assertOnAllCachesAndOwnership("k1", "computed_k1_value");

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      SerializableBiFunction<Object, Object, String> mappingToException = (k, v) -> {
         throw computeRaisedException;
      };
      expectException(RemoteException.class, () -> getFirstNonOwner("k1").computeIfPresent("k1", mappingToException));

      // remove if after compute value is null
      retval = getFirstNonOwner("k1").computeIfPresent("k1", (v1, v2) -> null);
      asyncWait("k1", ComputeCommand.class);
      if (testRetVals) assertNull(retval);
      assertRemovedOnAllCaches("k1");

      // do nothing if absent
      retval = getFirstNonOwner("notThere").computeIfPresent("notThere", (k, v) -> "add_" + k);
      asyncWait("notThere", ComputeCommand.class);
      if (testRetVals) assertNull(retval);
      assertRemovedOnAllCaches("notThere");
   }

   public void testComputeIfAbsentFromNonOwner() throws InterruptedException {
      // do nothing if value exists
      initAndTest();
      Object retval = getFirstNonOwner("k1").computeIfAbsent("k1", (k) -> "computed_" + k);
      if (testRetVals) assertEquals("value", retval);
      asyncWait("k1", ComputeIfAbsentCommand.class);
      assertOnAllCachesAndOwnership("k1", "value");

      // Compute key and add result value if absent
      retval = getFirstNonOwner("notExists").computeIfAbsent("notExists", (k) -> "computed_" + k);
      eventually(() -> {
         try {
            assertOnAllCachesAndOwnership("notExists", "computed_notExists");
         } catch (AssertionError e) {
            log.debugf("Assertion failed once", e);
            return false;
         }
         return true;
      });
      if (testRetVals) assertEquals("computed_notExists", retval);

      // do nothing if function result is null
      retval = getFirstNonOwner("doNothing").computeIfAbsent("doNothing", k -> null);
      asyncWait("doNothing", ComputeIfAbsentCommand.class);
      if (testRetVals) assertNull(retval);
      assertRemovedOnAllCaches("doNothing");

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      SerializableFunction<Object, String> mappingToException = k -> {
         throw computeRaisedException;
      };
      expectException(RemoteException.class, () -> getFirstNonOwner("somethingWrong").computeIfAbsent("somethingWrong", mappingToException));
   }
}
