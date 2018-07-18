package org.infinispan.functional;

import static org.infinispan.functional.FunctionalTestUtils.supplyIntKey;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ObjectInput;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Supplier;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.functional.decorators.FunctionalJCache;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test suite for verifying that the jcache implementation
 * based on functional map behaves in the correct way.
 */
@Test(groups = "functional", testName = "functional.FunctionalJCacheTest")
public class FunctionalJCacheTest extends AbstractFunctionalTest {

   Cache<Integer, String> local1;
   Cache<Integer, String> local2;

   Cache<Object, String> dist1;
   Cache<Object, String> dist2;

   Cache<Object, String> repl1;
   Cache<Object, String> repl2;

   @BeforeClass
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      local1 = FunctionalJCache.create(fmapL1);
      local2 = FunctionalJCache.create(fmapL2);
      dist1 = FunctionalJCache.create(fmapD1);
      dist2 = FunctionalJCache.create(fmapD2);
      repl1 = FunctionalJCache.create(fmapR1);
      repl2 = FunctionalJCache.create(fmapR2);
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
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key = keySupplier.get();
      assertEquals(null, readCache.get(key));
      writeCache.put(key, "one");
      assertEquals("one", readCache.get(key));
   }

   public void testLocalPutGet() {
      doPutGet(supplyIntKey(), local1, local2);
   }

   public void testReplPutGetOnNonOwner() {
      doPutGet(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplPutGetOnOwner() {
      doPutGet(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistPutGetOnNonOwner() {
      doPutGet(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistPutGetOnOwner() {
      doPutGet(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doPutGet(Supplier<K> keySupplier,
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key = keySupplier.get();
      assertEquals(null, writeCache.getAndPut(key, "one"));
      assertEquals("one", readCache.get(key));
   }

   public void testLocalGetAndPut() {
      doGetAndPut(supplyIntKey(), local1, local2);
   }

   public void testReplGetAndPutOnNonOwner() {
      doGetAndPut(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplGetAndPutOnOwner() {
      doGetAndPut(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistGetAndPutOnNonOwner() {
      doGetAndPut(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistGetAndPutOnOwner() {
      doGetAndPut(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doGetAndPut(Supplier<K> keySupplier,
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key = keySupplier.get();
      assertEquals(null, writeCache.getAndPut(key, "one"));
      assertEquals("one", writeCache.getAndPut(key, "uno"));
      assertEquals("uno", readCache.get(key));
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
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key1 = keySupplier.get(), key2 = keySupplier.get();
      assertFalse(writeCache.remove(key1));
      assertEquals(null, writeCache.getAndRemove(key1));
      assertEquals(null, writeCache.getAndPut(key1, "one"));
      assertEquals("one", readCache.get(key1));
      assertTrue(writeCache.remove(key1));
      assertEquals(null, readCache.get(key1));
      assertEquals(null, writeCache.getAndPut(key2, "two"));
      assertEquals("two", readCache.get(key2));
      assertEquals("two", writeCache.getAndRemove(key2));
      assertEquals(null, readCache.get(key2));
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
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key = keySupplier.get();
      assertEquals(false, readCache.containsKey(key));
      assertEquals(null, writeCache.getAndPut(key, "one"));
      assertEquals(true, readCache.containsKey(key));
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

   private <K> void doClear(Supplier<K> keySupplier, Cache<K, String> map1, Cache<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "two");
      map2.putAll(data);
      map2.clear();
      assertEquals(null, map1.get(key1));
      assertEquals(null, map1.get(key2));
      assertEquals(null, map1.get(key3));
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
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key = keySupplier.get();
      assertEquals(null, readCache.get(key));
      assertTrue(writeCache.putIfAbsent(key, "one"));
      assertEquals("one", readCache.get(key));
      assertFalse(writeCache.putIfAbsent(key, "uno"));
      assertEquals("one", readCache.get(key));
      assertTrue(writeCache.remove(key));
      assertEquals(null, readCache.get(key));
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
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key = keySupplier.get();
      assertEquals(null, readCache.get(key));
      assertFalse(writeCache.remove(key, "xxx"));
      assertEquals(null, writeCache.getAndPut(key, "one"));
      assertEquals("one", readCache.get(key));
      assertFalse(writeCache.remove(key, "xxx"));
      assertEquals("one", readCache.get(key));
      assertTrue(writeCache.remove(key, "one"));
      assertEquals(null, readCache.get(key));
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
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key = keySupplier.get();
      assertEquals(null, readCache.get(key));
      assertFalse(writeCache.replace(key, "xxx"));
      assertEquals(null, writeCache.getAndPut(key, "one"));
      assertEquals("one", readCache.get(key));
      assertTrue(writeCache.replace(key, "uno"));
      assertEquals("uno", readCache.get(key));
      assertTrue(writeCache.remove(key));
      assertEquals(null, readCache.get(key));
   }

   public void testLocalGetAndReplace() {
      doGetAndReplace(supplyIntKey(), local1, local2);
   }

   public void testReplGetAndReplaceOnNonOwner() {
      doGetAndReplace(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplGetAndReplaceOnOwner() {
      doGetAndReplace(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistGetAndReplaceOnNonOwner() {
      doGetAndReplace(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistGetAndReplaceOnOwner() {
      doGetAndReplace(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doGetAndReplace(Supplier<K> keySupplier,
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key = keySupplier.get();
      assertEquals(null, readCache.get(key));
      assertEquals(null, writeCache.getAndReplace(key, "xxx"));
      assertEquals(null, writeCache.getAndPut(key, "one"));
      assertEquals("one", readCache.get(key));
      assertEquals("one", writeCache.getAndReplace(key, "uno"));
      assertEquals("uno", readCache.get(key));
      assertTrue(writeCache.remove(key));
      assertEquals(null, readCache.get(key));
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
         Cache<K, String> readCache, Cache<K, String> writeCache) {
      K key = keySupplier.get();
      assertEquals(null, readCache.get(key));
      assertFalse(writeCache.replace(key, "xxx", "uno"));
      assertEquals(null, writeCache.getAndPut(key, "one"));
      assertEquals("one", readCache.get(key));
      assertFalse(writeCache.replace(key, "xxx", "uno"));
      assertEquals("one", readCache.get(key));
      assertTrue(writeCache.replace(key, "one", "uno"));
      assertEquals("uno", readCache.get(key));
      assertTrue(writeCache.remove(key));
      assertEquals(null, readCache.get(key));
   }

   public void testLocalPutAll() {
      doPutAllGetAllRemoveAll(supplyIntKey(), local1, local2);
   }

   public void testReplPutAllOnNonOwner() {
      doPutAllGetAllRemoveAll(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplPutAllOnOwner() {
      doPutAllGetAllRemoveAll(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistPutAllOnNonOwner() {
      doPutAllGetAllRemoveAll(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistPutAllOnOwner() {
      doPutAllGetAllRemoveAll(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doPutAllGetAllRemoveAll(Supplier<K> keySupplier,
         Cache<K, String> map1, Cache<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get(),
         key4 = keySupplier.get(), key5 = keySupplier.get(), key6 = keySupplier.get();
      assertTrue(map1.getAll(new HashSet<>(Arrays.asList(key1, key2, key3))).isEmpty());
      assertTrue(map1.getAll(new HashSet<>()).isEmpty());

      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "three");
      data.put(key4, "four");
      data.put(key5, "five");
      data.put(key6, "five");
      map2.putAll(data);

      assertEquals("one", map1.get(key1));
      assertEquals("two", map1.get(key2));
      assertEquals("three", map1.get(key3));
      assertEquals("four", map1.get(key4));
      assertEquals("five", map1.get(key5));
      assertEquals("five", map1.get(key6));

      // Get all no keys
      Map<K, String> res0 = map1.getAll(new HashSet<>());
      assertTrue(res0.isEmpty());
      assertEquals(0, res0.size());

      // Get all for a subset of keys
      Map<K, String> res1 = map1.getAll(new HashSet<>(Arrays.asList(key1, key2, key5, key6)));
      assertFalse(res1.isEmpty());

      assertEquals(4, res1.size());
      assertEquals("one", res1.get(key1));
      assertEquals("two", res1.get(key2));
      assertEquals("five", res1.get(key5));
      assertEquals("five", res1.get(key6));

      // Get all for entire keys set
      Map<K, String> res2 = map1.getAll(
         new HashSet<>(Arrays.asList(key1, key2, key3, key4, key5, key6)));
      assertFalse(res2.isEmpty());
      assertEquals(6, res2.size());
      assertEquals("one", res2.get(key1));
      assertEquals("two", res2.get(key2));
      assertEquals("three", res2.get(key3));
      assertEquals("four", res2.get(key4));
      assertEquals("five", res2.get(key5));
      assertEquals("five", res2.get(key6));

      // Remove all passing no keys
      map2.removeAll(new HashSet<>());
      Map<K, String> res3 = map1.getAll(
         new HashSet<>(Arrays.asList(key1, key2, key3, key4, key5, key6)));
      assertFalse(res3.isEmpty());
      assertEquals(6, res3.size());

      // Remove all passing subset of keys
      map2.removeAll(new HashSet<>(Arrays.asList(key3, key4, key5, key6)));
      Map<K, String> res4 = map1.getAll(
         new HashSet<>(Arrays.asList(key1, key2, key3, key4, key5, key6)));
      assertFalse(res4.isEmpty());
      assertEquals(2, res4.size());
      assertEquals("one", res4.get(key1));
      assertEquals("two", res4.get(key2));

      // Remove all the keys
      map2.removeAll();
      Map<K, String> res5 = map1.getAll(
         new HashSet<>(Arrays.asList(key1, key2, key3, key4, key5, key6)));
      assertTrue(res5.isEmpty());
      assertEquals(0, res5.size());
   }

   public void testLocalIterator() {
      doIterator(supplyIntKey(), local1, local2);
   }

   public void testReplIteratorOnNonOwner() {
      doIterator(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplIteratorOnOwner() {
      doIterator(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistIteratorOnNonOwner() {
      doIterator(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistIteratorOnOwner() {
      doIterator(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doIterator(Supplier<K> keySupplier,
         Cache<K, String> map1, Cache<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get(),
         key4 = keySupplier.get(), key5 = keySupplier.get(), key6 = keySupplier.get();
      assertFalse(map1.iterator().hasNext());
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "three");
      data.put(key4, "four");
      data.put(key5, "five");
      data.put(key6, "five");
      map2.putAll(data);

      Map<K, String> res0 = new HashMap<>();
      for (Cache.Entry<K, String> e : map1)
         res0.put(e.getKey(), e.getValue());

      assertEquals(data, res0);
      map2.clear();
   }

   public void testLocalInvoke() {
      doInvoke(supplyIntKey(), local1, local2);
   }

   public void testReplInvokeOnNonOwner() {
      doInvoke(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplInvokeOnOwner() {
      doInvoke(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistInvokeOnNonOwner() {
      doInvoke(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistInvokeOnOwner() {
      doInvoke(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doInvoke(Supplier<K> keySupplier,
         Cache<K, String> map1, Cache<K, String> map2) {
      K key = keySupplier.get();
      // Get via invoke
      String res0 = map1.invoke(key, GetValueProcessor.getInstance());
      assertEquals(null, res0);

      // Put via invoke
      map2.invoke(key, SetFirstArgValueProcessor.getInstance(), "one");

      // Get via invoke
      String res1 = map1.invoke(key, GetValueProcessor.getInstance());
      assertEquals("one", res1);

      // Remove via invoke
      map2.invoke(key, RemoveProcessor.getInstance());

      // Get via invoke
      String res2 = map1.invoke(key, GetValueProcessor.getInstance());
      assertEquals(null, res2);
   }

   @SerializeWith(value = GetValueProcessor.Externalizer0.class)
   private static final class GetValueProcessor<K, V> implements EntryProcessor<K, V, V> {
      @Override
      public V process(MutableEntry<K, V> entry, Object... args) throws EntryProcessorException {
         return entry.getValue();
      }

      private static final GetValueProcessor INSTANCE = new GetValueProcessor<>();

      @SuppressWarnings("unchecked")
      private static <K, V> GetValueProcessor<K, V> getInstance() {
         return INSTANCE;
      }

      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(UserObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = SetFirstArgValueProcessor.Externalizer0.class)
   private static final class SetFirstArgValueProcessor<K> implements EntryProcessor<K, String, Void> {
      @Override
      public Void process(MutableEntry<K, String> entry, Object... args) throws EntryProcessorException {
         entry.setValue((String) args[0]);
         return null;
      }

      private static final SetFirstArgValueProcessor INSTANCE = new SetFirstArgValueProcessor<>();

      @SuppressWarnings("unchecked")
      private static <K> SetFirstArgValueProcessor<K> getInstance() {
         return INSTANCE;
      }

      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(UserObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = RemoveProcessor.Externalizer0.class)
   private static final class RemoveProcessor<K, V> implements EntryProcessor<K, V, Void> {
      @Override
      public Void process(MutableEntry<K, V> entry, Object... args) throws EntryProcessorException {
         entry.remove();
         return null;
      }

      private static final RemoveProcessor INSTANCE = new RemoveProcessor<>();

      @SuppressWarnings("unchecked")
      private static <K, V> RemoveProcessor<K, V> getInstance() {
         return INSTANCE;
      }

      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(UserObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   public void testLocalInvokeAll() {
      doInvokeAll(supplyIntKey(), local1, local2);
   }

   public void testReplInvokeAllOnNonOwner() {
      doInvokeAll(supplyKeyForCache(0, REPL), repl1, repl2);
   }

   public void testReplInvokeAllOnOwner() {
      doInvokeAll(supplyKeyForCache(1, REPL), repl1, repl2);
   }

   public void testDistInvokeAllOnNonOwner() {
      doInvokeAll(supplyKeyForCache(0, DIST), dist1, dist2);
   }

   public void testDistInvokeAllOnOwner() {
      doInvokeAll(supplyKeyForCache(1, DIST), dist1, dist2);
   }

   private <K> void doInvokeAll(Supplier<K> keySupplier,
         Cache<K, String> map1, Cache<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      HashSet<K> keys = new HashSet<>(Arrays.asList(key1, key2, key3));

      // Get multi via invokeAll
      Map<K, EntryProcessorResult<String>> res0 = map1.invokeAll(keys, GetValueProcessor.getInstance());
      assertEquals(null, res0.get(key1).get());
      assertEquals(null, res0.get(key2).get());
      assertEquals(null, res0.get(key3).get());

      // Put multi via invokeAll
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "three");
      map2.invokeAll(keys, SetArgsValuesProcessor.getInstance(), data);

      // Get multi via invokeAll
      Map<K, EntryProcessorResult<String>> res1 = map1.invokeAll(keys, GetValueProcessor.getInstance());
      assertEquals("one", res1.get(key1).get());
      assertEquals("two", res1.get(key2).get());
      assertEquals("three", res1.get(key3).get());

      // Remove multi via invokeAll
      map2.invokeAll(keys, RemoveProcessor.getInstance());

      // Get multi via invokeAll
      Map<K, EntryProcessorResult<String>> res2 = map1.invokeAll(keys, GetValueProcessor.getInstance());
      assertEquals(null, res2.get(key1).get());
      assertEquals(null, res2.get(key2).get());
      assertEquals(null, res2.get(key3).get());
   }

   @SerializeWith(value = SetArgsValuesProcessor.Externalizer0.class)
   private static final class SetArgsValuesProcessor<K> implements EntryProcessor<K, String, Void> {
      @Override
      public Void process(MutableEntry<K, String> entry, Object... args) throws EntryProcessorException {
         Map<K, String> data = (Map<K, String>) args[0];
         entry.setValue(data.get(entry.getKey()));
         return null;
      }

      private static final SetArgsValuesProcessor INSTANCE = new SetArgsValuesProcessor<>();

      @SuppressWarnings("unchecked")
      private static <K> SetArgsValuesProcessor<K> getInstance() {
         return INSTANCE;
      }

      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(UserObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   public void testClose() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() throws Exception {
            AdvancedCache<Integer, String> advCache = cm.<Integer, String>getCache().getAdvancedCache();
            Cache<Integer, String> local = FunctionalJCache.create(advCache);
            assertFalse(local.isClosed());
            local.close();
            assertTrue(local.isClosed());
         }
      });
   }

   public void testGetName() {
      assertEquals("", local1.getName());
   }

}
