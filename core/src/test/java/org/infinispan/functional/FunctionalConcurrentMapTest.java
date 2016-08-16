package org.infinispan.functional;

import static org.infinispan.functional.FunctionalTestUtils.supplyIntKey;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.infinispan.functional.decorators.FunctionalConcurrentMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test suite for verifying that the concurrent map implementation
 * based on functional map behaves in the correct way.
 */
@Test(groups = "functional", testName = "functional.FunctionalConcurrentMapTest")
public class FunctionalConcurrentMapTest extends AbstractFunctionalTest {

   ConcurrentMap<Integer, String> local1;
   ConcurrentMap<Integer, String> local2;

   ConcurrentMap<Object, String> dist1;
   ConcurrentMap<Object, String> dist2;

   ConcurrentMap<Object, String> repl1;
   ConcurrentMap<Object, String> repl2;

   @BeforeClass
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      local1 = FunctionalConcurrentMap.create(cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache());
      local2 = FunctionalConcurrentMap.create(cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache());
      dist1 = FunctionalConcurrentMap.create(cacheManagers.get(0).<Object, String>getCache(DIST).getAdvancedCache());
      dist2 = FunctionalConcurrentMap.create(cacheManagers.get(1).<Object, String>getCache(DIST).getAdvancedCache());
      repl1 = FunctionalConcurrentMap.create(cacheManagers.get(0).<Object, String>getCache(REPL).getAdvancedCache());
      repl2 = FunctionalConcurrentMap.create(cacheManagers.get(1).<Object, String>getCache(REPL).getAdvancedCache());
   }

   public void testLocalEmptyGetThenPut() {
      doEmptyGetThenPut(supplyIntKey(), local1, local2);
   }

   public void testReplEmptyGetThenPutOnNonOwner() {
      doEmptyGetThenPut(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplEmptyGetThenPutOnOwner() {
      doEmptyGetThenPut(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistEmptyGetThenPutOnNonOwner() {
      doEmptyGetThenPut(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistEmptyGetThenPutOnOwner() {
      doEmptyGetThenPut(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doEmptyGetThenPut(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key = keySupplier.get();
      assertEquals(null, readMap.get(key));
      assertEquals(null, writeMap.put(key, "one"));
      assertEquals("one", readMap.get(key));
   }

   public void testLocalPutGet() {
      doPutGet(supplyIntKey(), local1, local2);
   }

   public void testReplPutGetOnNonOwner() {
      doPutGet(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testReplPutGetOnOwner() {
      doPutGet(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testDistPutGetOnNonOwner() {
      doPutGet(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistPutGetOnOwner() {
      doPutGet(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doPutGet(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key = keySupplier.get();
      assertEquals(null, writeMap.put(key, "one"));
      assertEquals("one", readMap.get(key));
   }

   public void testLocalPutUpdate() {
      doPutUpdate(supplyIntKey(), local1, local2);
   }

   public void testReplPutUpdateOnNonOwner() {
      doPutUpdate(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplPutUpdateOnOwner() {
      doPutUpdate(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testDistPutUpdateOnNonOwner() {
      doPutUpdate(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistPutUpdateOnOwner() {
      doPutUpdate(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doPutUpdate(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key = keySupplier.get();
      assertEquals(null, writeMap.put(key, "one"));
      assertEquals("one", writeMap.put(key, "uno"));
      assertEquals("uno", readMap.get(key));
   }

   public void testLocalGetAndRemove() {
      doGetAndRemove(supplyIntKey(), local1, local2);
   }

   public void testReplGetAndRemoveOnNonOwner() {
      doGetAndRemove(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplGetAndRemoveOnOwner() {
      doGetAndRemove(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistGetAndRemoveOnNonOwner() {
      doGetAndRemove(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistGetAndRemoveOnOwner() {
      doGetAndRemove(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doGetAndRemove(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key = keySupplier.get();
      assertEquals(null, writeMap.put(key, "one"));
      assertEquals("one", readMap.get(key));
      assertEquals("one", writeMap.remove(key));
      assertEquals(null, readMap.get(key));
   }

   public void testLocalContainsKey() {
      doContainsKey(supplyIntKey(), local1, local2);
   }

   public void testReplContainsKeyOnNonOwner() {
      doContainsKey(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplContainsKeyOnOwner() {
      doContainsKey(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistContainsKeyOnNonOwner() {
      doContainsKey(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistContainsKeyOnOwner() {
      doContainsKey(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doContainsKey(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(false, map1.containsKey(key));
      assertEquals(null, map2.put(key, "one"));
      assertEquals(true, map1.containsKey(key));
   }

   public void testLocalContainsValue() {
      doContainsValue(supplyIntKey(), "one", local1, local2);
   }

   public void testReplContainsValueOnNonOwner() {
      doContainsValue(supplyKeyForCache(0, REPL), "one", repl1, repl2);
   }

   public void testReplContainsValueOnOwner() {
      doContainsValue(supplyKeyForCache(1, REPL), "one", repl1, repl2);
   }

   public void testDistContainsValueOnNonOwner() {
      doContainsValue(supplyKeyForCache(0, DIST), "one", dist1, dist2);
   }

   public void testDistContainsValueOnOwner() {
      doContainsValue(supplyKeyForCache(1, DIST), "one", dist1, dist2);
   }

   private <K> void doContainsValue(Supplier<K> keySupplier, String value,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(false, map1.containsValue(value));
      assertEquals(null, map2.put(key, value));
      assertEquals(true, map1.containsValue(value));
      assertEquals(false, map1.containsValue("xxx"));
   }

   public void testLocalSize() {
      doSize(supplyIntKey(), local1, local2);
   }

   public void testReplSizeOnNonOwner() {
      doSize(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplSizeOnOwner() {
      doSize(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistSizeOnNonOwner() {
      doSize(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistSizeOnOwner() {
      doSize(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doSize(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key1 = keySupplier.get(), key2 = keySupplier.get();
      assertEquals(0, readMap.size());
      assertEquals(null, writeMap.put(key1, "one"));
      assertEquals(1, readMap.size());
      assertEquals(null, writeMap.put(key2, "two"));
      assertEquals(2, readMap.size());
      assertEquals("one", writeMap.remove(key1));
      assertEquals(1, writeMap.size());
      assertEquals("two", writeMap.remove(key2));
      assertEquals(0, writeMap.size());
   }

   public void testLocalEmpty() {
      doEmpty(supplyIntKey(), local1, local2);
   }

   public void testReplEmptyOnNonOwner() {
      doEmpty(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplEmptyOnOwner() {
      doEmpty(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistEmptyOnNonOwner() {
      doEmpty(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistEmptyOnOwner() {
      doEmpty(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doEmpty(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key = keySupplier.get();
      assertEquals(true, readMap.isEmpty());
      assertEquals(null, writeMap.put(key, "one"));
      assertEquals("one", readMap.get(key));
      assertEquals(false, readMap.isEmpty());
      assertEquals("one", writeMap.remove(key));
      assertEquals(true, readMap.isEmpty());
   }

   public void testLocalPutAll() {
      doPutAll(supplyIntKey(), local1, local2);
   }

   public void testReplPutAllOnNonOwner() {
      doPutAll(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplPutAllOnOwner() {
      doPutAll(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistPutAllOnNonOwner() {
      doPutAll(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistPutAllOnOwner() {
      doPutAll(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doPutAll(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      assertEquals(true, readMap.isEmpty());
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "two");
      writeMap.putAll(data);
      assertEquals("one", readMap.get(key1));
      assertEquals("two", readMap.get(key2));
      assertEquals("two", readMap.get(key3));
      assertEquals("one", writeMap.remove(key1));
      assertEquals("two", writeMap.remove(key2));
      assertEquals("two", writeMap.remove(key3));
   }

   public void testLocalClear() {
      doClear(supplyIntKey(), local1, local2);
   }

   public void testReplClearOnNonOwner() {
      doClear(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplClearOnOwner() {
      doClear(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistClearOnNonOwner() {
      doClear(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistClearOnOwner() {
      doClear(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doClear(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      assertEquals(true, readMap.isEmpty());
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "two");
      writeMap.putAll(data);
      assertEquals("one", readMap.get(key1));
      assertEquals("two", readMap.get(key2));
      assertEquals("two", readMap.get(key3));
      writeMap.clear();
      assertEquals(null, readMap.get(key1));
      assertEquals(null, readMap.get(key2));
      assertEquals(null, readMap.get(key3));
   }

   public void testLocalKeyValueAndEntrySets() {
      doKeyValueAndEntrySets(supplyIntKey(), local1, local2);
   }

   public void testReplKeyValueAndEntrySetsOnNonOwner() {
      doKeyValueAndEntrySets(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplKeyValueAndEntrySetsOnOwner() {
      doKeyValueAndEntrySets(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistKeyValueAndEntrySetsOnNonOwner() {
      doKeyValueAndEntrySets(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistKeyValueAndEntrySetsOnOwner() {
      doKeyValueAndEntrySets(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doKeyValueAndEntrySets(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      assertEquals(true, readMap.isEmpty());
      writeMap.put(key1, "one");
      writeMap.put(key2, "two");
      writeMap.put(key3, "two");

      Set<K> keys = readMap.keySet();
      assertEquals(3, keys.size());
      Set<K> expectedKeys = new HashSet<>(Arrays.asList(key1, key2, key3));
      keys.forEach(expectedKeys::remove);
      assertEquals(true, expectedKeys.isEmpty());

      assertEquals(false, readMap.isEmpty());
      Collection<String> values = readMap.values();
      assertEquals(3, values.size());
      Set<String> expectedValues = new HashSet<>(Arrays.asList("one", "two"));
      values.forEach(expectedValues::remove);
      assertEquals(true, expectedValues.isEmpty());

      Set<Map.Entry<K, String>> entries = readMap.entrySet();
      assertEquals(3, entries.size());
      entries.forEach(e -> {
         if (e.getKey().equals(key1)) e.setValue("uno");
         else if (e.getKey().equals(key2)) e.setValue("dos");
         else e.setValue("dos");
      });

      assertEquals("uno", writeMap.remove(key1));
      assertEquals("dos", writeMap.remove(key2));
      assertEquals("dos", writeMap.remove(key3));
   }

   public void testLocalPutIfAbsent() {
      doPutIfAbsent(supplyIntKey(), local1, local2);
   }

   public void testReplPutIfAbsentOnNonOwner() {
      doPutIfAbsent(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplPutIfAbsentOnOwner() {
      doPutIfAbsent(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistPutIfAbsentOnNonOwner() {
      doPutIfAbsent(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistPutIfAbsentOnOwner() {
      doPutIfAbsent(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doPutIfAbsent(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key = keySupplier.get();
      assertEquals(null, readMap.get(key));
      assertEquals(null, writeMap.putIfAbsent(key, "one"));
      assertEquals("one", readMap.get(key));
      assertEquals("one", writeMap.putIfAbsent(key, "uno"));
      assertEquals("one", readMap.get(key));
      assertEquals("one", writeMap.remove(key));
      assertEquals(null, readMap.get(key));
   }

   public void testLocalConditionalRemove() {
      doConditionalRemove(supplyIntKey(), local1, local2);
   }

   public void testReplConditionalRemoveOnNonOwner() {
      doConditionalRemove(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplConditionalRemoveOnOwner() {
      doConditionalRemove(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistConditionalRemoveOnNonOwner() {
      doConditionalRemove(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistConditionalRemoveOnOwner() {
      doConditionalRemove(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doConditionalRemove(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key = keySupplier.get();
      assertEquals(null, readMap.get(key));
      assertFalse(writeMap.remove(key, "xxx"));
      assertEquals(null, writeMap.put(key, "one"));
      assertEquals("one", readMap.get(key));
      assertFalse(writeMap.remove(key, "xxx"));
      assertEquals("one", readMap.get(key));
      assertTrue(writeMap.remove(key, "one"));
      assertEquals(null, readMap.get(key));
   }

   public void testLocalReplace() {
      doReplace(supplyIntKey(), local1, local2);
   }

   public void testReplReplaceOnNonOwner() {
      doReplace(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplReplaceOnOwner() {
      doReplace(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistReplaceOnNonOwner() {
      doReplace(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistReplaceOnOwner() {
      doReplace(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doReplace(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key = keySupplier.get();
      assertEquals(null, readMap.get(key));
      assertEquals(null, writeMap.replace(key, "xxx"));
      assertEquals(null, writeMap.put(key, "one"));
      assertEquals("one", readMap.get(key));
      assertEquals("one", writeMap.replace(key, "uno"));
      assertEquals("uno", readMap.get(key));
      assertEquals("uno", writeMap.remove(key));
      assertEquals(null, readMap.get(key));
   }

   public void testLocalReplaceWithValue() {
      doReplaceWithValue(supplyIntKey(), local1, local2);
   }

   public void testReplReplaceWithValueOnNonOwner() {
      doReplaceWithValue(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplReplaceWithValueOnOwner() {
      doReplaceWithValue(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistReplaceWithValueOnNonOwner() {
      doReplaceWithValue(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistReplaceWithValueOnOwner() {
      doReplaceWithValue(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doReplaceWithValue(Supplier<K> keySupplier,
         ConcurrentMap<K, String> readMap,
         ConcurrentMap<K, String> writeMap) {
      K key = keySupplier.get();
      assertEquals(null, readMap.get(key));
      assertFalse(writeMap.replace(key, "xxx", "uno"));
      assertEquals(null, writeMap.put(key, "one"));
      assertEquals("one", readMap.get(key));
      assertFalse(writeMap.replace(key, "xxx", "uno"));
      assertEquals("one", readMap.get(key));
      assertTrue(writeMap.replace(key, "one", "uno"));
      assertEquals("uno", readMap.get(key));
      assertEquals("uno", writeMap.remove(key));
      assertEquals(null, readMap.get(key));
   }

}
