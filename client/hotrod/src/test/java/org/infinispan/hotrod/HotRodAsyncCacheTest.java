package org.infinispan.hotrod;

import static org.infinispan.hotrod.AwaitAssertions.assertAwaitEquals;
import static org.infinispan.hotrod.AwaitAssertions.await;
import static org.infinispan.hotrod.CacheEntryAssertions.assertEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.cache.CacheEntryImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryVersionImpl;
import org.infinispan.hotrod.test.AbstractAsyncCacheSingleServerTest;
import org.infinispan.hotrod.test.KeyValueGenerator;
import org.infinispan.hotrod.util.FlowUtils;
import org.infinispan.hotrod.util.MapKVHelper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * @since 14.0
 **/
public class HotRodAsyncCacheTest<K, V> extends AbstractAsyncCacheSingleServerTest<K, V> {

   @MethodSource("parameterized")
   @ParameterizedTest(name = "getPut[{0}]")
   public void testPut(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V v1 = kvGenerator.generateValue(cacheName, 0);

      final CacheWriteOptions options = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(15))
            .lifespanAndMaxIdle(Duration.ofSeconds(20), Duration.ofSeconds(25))
            .build();
      final CacheEntryVersion version1 = new CacheEntryVersionImpl(1);
      assertEntry(key, null, kvGenerator, await(cache.put(key, v1, options)));
      kvGenerator.assertValueEquals(v1, await(cache.get(key)));
      assertEntry(key, v1, kvGenerator, await(cache.getEntry(key)), options, version1);

      CacheWriteOptions optionsV1 = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(20))
            .lifespanAndMaxIdle(Duration.ofSeconds(25), Duration.ofSeconds(30))
            .build();
      final CacheEntryVersion version2 = new CacheEntryVersionImpl(2);
      final V v2 = kvGenerator.generateValue(cacheName, 1);
      assertEntry(key, v1, kvGenerator, await(cache.put(key, v2, optionsV1)), options, version1);
      assertEntry(key, v2, kvGenerator, await(cache.getEntry(key)), optionsV1, version2);
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testPutIfAbsent[{0}]")
   public void testPutIfAbsent(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V v1 = kvGenerator.generateValue(cacheName, 0);

      final CacheWriteOptions options = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(15))
            .lifespanAndMaxIdle(Duration.ofSeconds(20), Duration.ofSeconds(25))
            .build();
      final CacheEntryVersion version1 = new CacheEntryVersionImpl(1);
      assertAwaitEquals(null, cache.putIfAbsent(key, v1, options));
      assertEntry(key, v1, kvGenerator, await(cache.getEntry(key)), options, version1);

      final V other = kvGenerator.generateValue(cacheName, 1);
      CacheEntry<K, V> previousEntry = await(cache.putIfAbsent(key, other));
      kvGenerator.assertKeyEquals(key, previousEntry.key());

      kvGenerator.assertValueEquals(v1, previousEntry.value());
      kvGenerator.assertValueEquals(v1, await(cache.get(key)));
      assertEntry(key, v1, kvGenerator, await(cache.getEntry(key)), options, version1);
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testSetIfAbsent[{0}]")
   public void testSetIfAbsent(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V value = kvGenerator.generateValue(cacheName, 0);

      CacheWriteOptions options = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(15))
            .lifespanAndMaxIdle(Duration.ofSeconds(20), Duration.ofSeconds(25))
            .build();
      assertAwaitEquals(true, cache.setIfAbsent(key, value, options));
      assertEntry(key, value, kvGenerator, await(cache.getEntry(key)), options, new CacheEntryVersionImpl(1));

      final V other = kvGenerator.generateValue(cacheName, 1);
      assertAwaitEquals(false, cache.setIfAbsent(key, other));

      final V actual = await(cache.get(key));
      kvGenerator.assertValueEquals(value, actual);

      assertEntry(key, value, kvGenerator, await(cache.getEntry(key)), options, new CacheEntryVersionImpl(1));
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testSet[{0}]")
   public void testSet(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V v1 = kvGenerator.generateValue(cacheName, 0);

      final CacheWriteOptions options = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(15))
            .lifespanAndMaxIdle(Duration.ofSeconds(20), Duration.ofSeconds(25))
            .build();
      final CacheEntryVersion version1 = new CacheEntryVersionImpl(1);
      await(cache.set(key, v1, options));
      assertEntry(key, v1, kvGenerator, await(cache.getEntry(key)), options, version1);

      final CacheWriteOptions optionsV2 = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(20))
            .lifespanAndMaxIdle(Duration.ofSeconds(25), Duration.ofSeconds(30))
            .build();
      final CacheEntryVersion version2 = new CacheEntryVersionImpl(2);
      final V v2 = kvGenerator.generateValue(cacheName, 1);
      await(cache.set(key, v2, optionsV2));
      V actual = await(cache.get(key));
      kvGenerator.assertValueEquals(v2, actual);

      assertEntry(key, v2, kvGenerator, await(cache.getEntry(key)), optionsV2, version2);
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testGetAndRemove[{0}]")
   public void testGetAndRemove(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V value = kvGenerator.generateValue(cacheName, 0);
      final CacheWriteOptions options = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(15))
            .lifespanAndMaxIdle(Duration.ofSeconds(20), Duration.ofSeconds(25))
            .build();
      final CacheEntryVersion cev = new CacheEntryVersionImpl(1);
      assertEntry(key, null, kvGenerator, await(cache.put(key, value, options)));

      assertEntry(key, value, kvGenerator, await(cache.getEntry(key)), options, cev);
      assertEntry(key, value, kvGenerator, await(cache.getAndRemove(key)), options, cev);

      assertAwaitEquals(null, cache.get(key));
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testPutAllAndClear[{0}]")
   public void testPutAllAndClear(KeyValueGenerator<K, V> kvGenerator) {
      Map<K, V> entries = new HashMap<>();
      for (int i = 0; i < 10; i++) {
         final K key = kvGenerator.generateKey(cacheName, i);
         final V value = kvGenerator.generateValue(cacheName, i);
         entries.put(key, value);
      }

      final CacheWriteOptions options = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(15))
            .lifespanAndMaxIdle(Duration.ofSeconds(20), Duration.ofSeconds(25))
            .build();
      await(cache.putAll(entries, options));

      final CacheEntryVersion cve = new CacheEntryVersionImpl(0);
      for (Map.Entry<K, V> entry : entries.entrySet()) {
         assertEntry(entry.getKey(), entry.getValue(), kvGenerator, await(cache.getEntry(entry.getKey())), options, cve);
      }

      await(cache.clear());
      for (Map.Entry<K, V> entry : entries.entrySet()) {
         assertAwaitEquals(null, cache.get(entry.getKey()));
      }
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testPutAllGetAll[{0}]")
   public void testPutAllGetAll(KeyValueGenerator<K, V> kvGenerator) {
      Map<K, V> entries = new HashMap<>();
      for (int i = 0; i < 10; i++) {
         final K key = kvGenerator.generateKey(cacheName, i);
         final V value = kvGenerator.generateValue(cacheName, i);
         entries.put(key, value);
      }

      final CacheWriteOptions options = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(15))
            .lifespanAndMaxIdle(Duration.ofSeconds(20), Duration.ofSeconds(25))
            .build();
      await(cache.putAll(entries, options));

      Flow.Publisher<CacheEntry<K, V>> publisher = cache.getAll(entries.keySet());
      Map<K, CacheEntry<K, V>> retrieved = FlowUtils.blockingCollect(publisher)
            .stream().collect(Collectors.toMap(CacheEntry::key, e -> e));

      assertEquals(entries.size(), retrieved.size());
      MapKVHelper<K, V> helper = new MapKVHelper<>(entries, kvGenerator);
      for (Map.Entry<K, CacheEntry<K, V>> entry : retrieved.entrySet()) {
         V expected = helper.get(entry.getKey());
         assertNotNull(expected);
         // TODO: once handling metadata on remote cache verify that too
         assertEntry(entry.getKey(), expected, kvGenerator, entry.getValue());
      }
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testPutAllGetAndRemoveAll[{0}]")
   public void testPutAllGetAndRemoveAll(KeyValueGenerator<K, V> kvGenerator) {
      Map<K, V> entries = new HashMap<>();
      SubmissionPublisher<CacheEntry<K, V>> entriesPublisher = new SubmissionPublisher<>();
      final CacheWriteOptions options = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(15))
            .lifespanAndMaxIdle(Duration.ofSeconds(20), Duration.ofSeconds(25))
            .build();
      CompletionStage<Void> putAll = cache.putAll(entriesPublisher, options);

      for (int i = 0; i < 10; i++) {
         final K key = kvGenerator.generateKey(cacheName, i);
         final V value = kvGenerator.generateValue(cacheName, i);
         entries.put(key, value);
         entriesPublisher.submit(new CacheEntryImpl<>(key, value, null));
      }

      entriesPublisher.close();
      await(putAll);

      Flow.Publisher<CacheEntry<K, V>> publisher = cache.getAndRemoveAll(entries.keySet());
      Map<K, CacheEntry<K, V>> retrieved = FlowUtils.blockingCollect(publisher)
            .stream().collect(Collectors.toMap(CacheEntry::key, e -> e));

      assertEquals(entries.size(), retrieved.size());
      final CacheEntryVersion cve = new CacheEntryVersionImpl(0);
      MapKVHelper<K, V> helper = new MapKVHelper<>(entries, kvGenerator);
      for (Map.Entry<K, CacheEntry<K, V>> entry : retrieved.entrySet()) {
         V expected = helper.get(entry.getKey());
         assertNotNull(expected);
         assertEntry(entry.getKey(), expected, kvGenerator, entry.getValue(), options, cve);
      }

      for (Map.Entry<K, V> entry : entries.entrySet()) {
         assertAwaitEquals(null, cache.get(entry.getKey()));
      }

      // Removing keys that dont exists returns nothing.
       retrieved = FlowUtils.blockingCollect(cache.getAndRemoveAll(entries.keySet()))
            .stream().collect(Collectors.toMap(CacheEntry::key, e -> e));
      assertTrue(retrieved.isEmpty());
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testReplace[{0}]")
   public void testReplace(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V initialValue = kvGenerator.generateValue(cacheName, 0);
      final CacheWriteOptions options = CacheWriteOptions.writeOptions()
            .timeout(Duration.ofSeconds(15))
            .lifespanAndMaxIdle(Duration.ofSeconds(20), Duration.ofSeconds(25))
            .build();

      // Returns false for an unexistent entry.
      final CacheEntryVersion cve0 = new CacheEntryVersionImpl(0);
      assertAwaitEquals(false, cache.replace(key, initialValue, cve0));
      assertEntry(key, null, kvGenerator, await(cache.put(key, initialValue, options)));
      assertEntry(key, initialValue, kvGenerator, await(cache.getEntry(key)));

      final V replaceValue = kvGenerator.generateValue(cacheName, 1);
      final CacheEntryVersion cve2 = new CacheEntryVersionImpl(2);
      assertAwaitEquals(true, cache.replace(key, replaceValue, cve2));

      // Returns false for the wrong version.
      final V anyValue = kvGenerator.generateValue(cacheName, 1);
      assertAwaitEquals(false, cache.replace(key, anyValue, cve0));
   }

   public Stream<Arguments> parameterized() {
      return Stream.of(
            Arguments.of(KeyValueGenerator.BYTE_ARRAY_GENERATOR),
            Arguments.of(KeyValueGenerator.STRING_GENERATOR),
            Arguments.of(KeyValueGenerator.GENERIC_ARRAY_GENERATOR)
      );
   }

}
