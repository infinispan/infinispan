package org.infinispan.functional;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.functional.decorators.FunctionalConcurrentMap;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static org.testng.AssertJUnit.*;

/**
 * Test suite for verifying that the concurrent map implementation
 * based on functional map behaves in the correct way.
 */
@Test(groups = "functional", testName = "functional.FunctionalConcurrentMapTest")
public class FunctionalConcurrentMapTest extends MultipleCacheManagersTest {

   private static final String DIST = "dist";
   private static final String REPL = "repl";
   private static final Random R = new Random();

   ConcurrentMap<Integer, String> local1;
   ConcurrentMap<Integer, String> local2;

   ConcurrentMap<Object, String> dist1;
   ConcurrentMap<Object, String> dist2;

   ConcurrentMap<Object, String> repl1;
   ConcurrentMap<Object, String> repl2;

   Supplier<Integer> supplyIntKey() {
      return () -> R.nextInt(Integer.MAX_VALUE);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      // Create local caches as default in a cluster of 2
      createClusteredCaches(2, new ConfigurationBuilder());
      // Create distributed caches
      ConfigurationBuilder distBuilder = new ConfigurationBuilder();
      distBuilder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(1);
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(DIST, distBuilder.build()));
      // Create replicated caches
      ConfigurationBuilder replBuilder = new ConfigurationBuilder();
      replBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(REPL, replBuilder.build()));
      // Wait for cluster to form
      waitForClusterToForm(DIST, REPL);
   }

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

   public void testReplEmptyGetThenPut() {
      doEmptyGetThenPut(supplyKeyForCache(0, REPL), repl1, repl2);
      doEmptyGetThenPut(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistEmptyGetThenPut() {
      doEmptyGetThenPut(supplyKeyForCache(0, DIST), dist1, dist2);
      doEmptyGetThenPut(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doEmptyGetThenPut(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(null, map1.get(key));
      assertEquals(null, map2.put(key, "one"));
      assertEquals("one", map1.get(key));
   }

   public void testLocalPutGet() {
      doPutGet(supplyIntKey(), local1, local2);
   }

   public void testReplPutGet() {
      doPutGet(supplyKeyForCache(0, REPL), repl1, repl2);
      doPutGet(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistPutGet() {
      doPutGet(supplyKeyForCache(0, DIST), dist1, dist2);
      doPutGet(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doPutGet(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(null, map1.put(key, "one"));
      assertEquals("one", map2.get(key));
   }

   public void testLocalGetAndPut() {
      doGetAndPut(supplyIntKey(), local1, local2);
   }

   public void testReplGetAndPut() {
      doGetAndPut(supplyKeyForCache(0, REPL), repl1, repl2);
      doGetAndPut(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistGetAndPut() {
      doGetAndPut(supplyKeyForCache(0, DIST), dist1, dist2);
      doGetAndPut(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doGetAndPut(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(null, map1.put(key, "one"));
      assertEquals("one", map2.put(key, "uno"));
      assertEquals("uno", map1.get(key));
   }

   public void testLocalGetAndRemove() {
      doGetAndRemove(supplyIntKey(), local1, local2);
   }

   public void testReplGetAndRemove() {
      doGetAndRemove(supplyKeyForCache(0, REPL), repl1, repl2);
      doGetAndRemove(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistGetAndRemove() {
      doGetAndRemove(supplyKeyForCache(0, DIST), dist1, dist2);
      doGetAndRemove(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doGetAndRemove(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(null, map1.put(key, "one"));
      assertEquals("one", map2.get(key));
      assertEquals("one", map2.remove(key));
      assertEquals(null, map1.get(key));
   }

   public void testLocalContainsKey() {
      doContainsKey(supplyIntKey(), local1, local2);
   }

   public void testReplContainsKey() {
      doContainsKey(supplyKeyForCache(0, REPL), repl1, repl2);
      doContainsKey(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistContainsKey() {
      doContainsKey(supplyKeyForCache(0, DIST), dist1, dist2);
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

   public void testReplContainsValue() {
      doContainsValue(supplyKeyForCache(0, REPL), "one", repl1, repl2);
      doContainsValue(supplyKeyForCache(1, REPL), "two", repl1, repl2);
   }

   public void testDistContainsValue() {
      doContainsValue(supplyKeyForCache(0, DIST), "one", dist1, dist2);
      doContainsValue(supplyKeyForCache(1, DIST), "two", dist1, dist2);
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

   public void testReplSize() {
      doSize(supplyKeyForCache(0, REPL), repl1, repl2);
      doSize(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistSize() {
      doSize(supplyKeyForCache(0, DIST), dist1, dist2);
      doSize(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doSize(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get();
      assertEquals(0, map1.size());
      assertEquals(null, map2.put(key1, "one"));
      assertEquals(1, map1.size());
      assertEquals(null, map2.put(key2, "two"));
      assertEquals(2, map1.size());
      assertEquals("two", map2.remove(key2));
      assertEquals(1, map2.size());
      assertEquals("one", map2.remove(key1));
      assertEquals(0, map2.size());
   }

   public void testLocalEmpty() {
      doEmpty(supplyIntKey(), local1, local2);
   }

   public void testReplEmpty() {
      doEmpty(supplyKeyForCache(0, REPL), repl1, repl2);
      doEmpty(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistEmpty() {
      doEmpty(supplyKeyForCache(0, DIST), dist1, dist2);
      doEmpty(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doEmpty(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(true, map1.isEmpty());
      assertEquals(null, map2.put(key, "one"));
      assertEquals("one", map1.get(key));
      assertEquals(false, map1.isEmpty());
      assertEquals("one", map2.remove(key));
      assertEquals(true, map1.isEmpty());
   }

   public void testLocalPutAll() {
      doPutAll(supplyIntKey(), local1, local2);
   }

   public void testReplPutAll() {
      doPutAll(supplyKeyForCache(0, REPL), repl1, repl2);
      doPutAll(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistPutAll() {
      doPutAll(supplyKeyForCache(0, DIST), dist1, dist2);
      doPutAll(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doPutAll(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      assertEquals(true, map1.isEmpty());
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "two");
      map2.putAll(data);
      assertEquals("one", map1.get(key1));
      assertEquals("two", map1.get(key2));
      assertEquals("two", map1.get(key3));
      assertEquals("one", map2.remove(key1));
      assertEquals("two", map2.remove(key2));
      assertEquals("two", map2.remove(key3));
   }

   public void testLocalClear() {
      doClear(supplyIntKey(), local1, local2);
   }

   public void testReplClear() {
      doClear(supplyKeyForCache(0, REPL), repl1, repl2);
      doClear(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistClear() {
      doClear(supplyKeyForCache(0, DIST), dist1, dist2);
      doClear(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doClear(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      assertEquals(true, map1.isEmpty());
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "two");
      map2.putAll(data);
      assertEquals("one", map1.get(key1));
      assertEquals("two", map1.get(key2));
      assertEquals("two", map1.get(key3));
      map2.clear();
      assertEquals(null, map1.get(key1));
      assertEquals(null, map1.get(key2));
      assertEquals(null, map1.get(key3));
   }

   public void testLocalKeyValueAndEntrySets() {
      doKeyValueAndEntrySets(supplyIntKey(), local1, local2);
   }

   public void testReplKeyValueAndEntrySets() {
      doKeyValueAndEntrySets(supplyKeyForCache(0, REPL), repl1, repl2);
      doKeyValueAndEntrySets(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistKeyValueAndEntrySets() {
      doKeyValueAndEntrySets(supplyKeyForCache(0, DIST), dist1, dist2);
      doKeyValueAndEntrySets(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doKeyValueAndEntrySets(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      assertEquals(true, map1.isEmpty());
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "two");
      map2.putAll(data);

      Set<K> keys = map1.keySet();
      assertEquals(3, keys.size());
      Set<K> expectedKeys = new HashSet<>(Arrays.asList(key1, key2));
      keys.forEach(expectedKeys::remove);
      assertEquals(true, expectedKeys.isEmpty());

      assertEquals(false, map1.isEmpty());
      Collection<String> values = map1.values();
      assertEquals(3, values.size());
      Set<String> expectedValues = new HashSet<>(Arrays.asList("one", "two"));
      values.forEach(expectedValues::remove);
      assertEquals(true, expectedValues.isEmpty());

      Set<Map.Entry<K, String>> entries = map1.entrySet();
      assertEquals(3, entries.size());
      entries.removeAll(data.entrySet());
      assertEquals(true, entries.isEmpty());

      assertEquals("one", map2.remove(key1));
      assertEquals("two", map2.remove(key2));
      assertEquals("two", map2.remove(key3));
   }

   public void testLocalPutIfAbsent() {
      doPutIfAbsent(supplyIntKey(), local1, local2);
   }

   public void testReplPutIfAbsent() {
      doPutIfAbsent(supplyKeyForCache(0, REPL), repl1, repl2);
      doPutIfAbsent(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistPutIfAbsent() {
      doPutIfAbsent(supplyKeyForCache(0, DIST), dist1, dist2);
      doPutIfAbsent(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doPutIfAbsent(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(null, map1.get(key));
      assertEquals(null, map2.putIfAbsent(key, "one"));
      assertEquals("one", map1.get(key));
      assertEquals("one", map2.putIfAbsent(key, "uno"));
      assertEquals("one", map1.get(key));
      assertEquals("one", map2.remove(key));
      assertEquals(null, map1.get(key));
   }

   public void testLocalConditionalRemove() {
      doConditionalRemove(supplyIntKey(), local1, local2);
   }

   public void testReplConditionalRemove() {
      doConditionalRemove(supplyKeyForCache(0, REPL), repl1, repl2);
      doConditionalRemove(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistConditionalRemove() {
      doConditionalRemove(supplyKeyForCache(0, DIST), dist1, dist2);
      doConditionalRemove(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doConditionalRemove(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(null, map1.get(key));
      assertFalse(map2.remove(key, "xxx"));
      assertEquals(null, map2.put(key, "one"));
      assertEquals("one", map1.get(key));
      assertFalse(map2.remove(key, "xxx"));
      assertEquals("one", map1.get(key));
      assertTrue(map2.remove(key, "one"));
      assertEquals(null, map1.get(key));
   }

   public void testLocalReplace() {
      doReplace(supplyIntKey(), local1, local2);
   }

   public void testReplReplace() {
      doReplace(supplyKeyForCache(0, REPL), repl1, repl2);
      doReplace(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistReplace() {
      doReplace(supplyKeyForCache(0, DIST), dist1, dist2);
      doReplace(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doReplace(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(null, map1.get(key));
      assertEquals(null, map2.replace(key, "xxx"));
      assertEquals(null, map2.put(key, "one"));
      assertEquals("one", map1.get(key));
      assertEquals("one", map2.replace(key, "uno"));
      assertEquals("uno", map1.get(key));
      assertEquals("uno", map2.remove(key));
      assertEquals(null, map1.get(key));
   }

   public void testLocalReplaceWithValue() {
      doReplaceWithValue(supplyIntKey(), local1, local2);
   }

   public void testReplReplaceWithValue() {
      doReplaceWithValue(supplyKeyForCache(0, REPL), repl1, repl2);
      doReplaceWithValue(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistReplaceWithValue() {
      doReplaceWithValue(supplyKeyForCache(0, DIST), dist1, dist2);
      doReplaceWithValue(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doReplaceWithValue(Supplier<K> keySupplier,
         ConcurrentMap<K, String> map1,
         ConcurrentMap<K, String> map2) {
      K key = keySupplier.get();
      assertEquals(null, map1.get(key));
      assertFalse(map2.replace(key, "xxx", "uno"));
      assertEquals(null, map2.put(key, "one"));
      assertEquals("one", map1.get(key));
      assertFalse(map2.replace(key, "xxx", "uno"));
      assertEquals("one", map1.get(key));
      assertTrue(map2.replace(key, "one", "uno"));
      assertEquals("uno", map1.get(key));
      assertEquals("uno", map2.remove(key));
      assertEquals(null, map1.get(key));
   }

}
