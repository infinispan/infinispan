package org.infinispan.hotrod;

import static org.infinispan.hotrod.AwaitAssertions.assertAwaitEquals;
import static org.infinispan.hotrod.AwaitAssertions.await;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.hotrod.test.AbstractSingleHotRodServerTest;
import org.infinispan.hotrod.test.KeyValueGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * @since 14.0
 **/
public class HotRodAsyncCacheTest<K, V> extends AbstractSingleHotRodServerTest<K, V> {

   @MethodSource("parameterized")
   @ParameterizedTest(name = "getPut[{0}]")
   public void testPut(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V v0 = kvGenerator.generateValue(cacheName, 0);
      assertEntry(key, null, kvGenerator, await(cache.put(key, v0)));
      kvGenerator.assertValueEquals(v0, await(cache.get(key)));

      final V v1 = kvGenerator.generateValue(cacheName, 1);
      assertEntry(key, v0, kvGenerator, await(cache.put(key, v1)));

      V actual = await(cache.get(key));
      kvGenerator.assertValueEquals(v1, actual);
      kvGenerator.assertValueNotEquals(v0, actual);

      assertEntry(key, v1, kvGenerator, await(cache.getEntry(key)));
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testPutIfAbsent[{0}]")
   public void testPutIfAbsent(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V v0 = kvGenerator.generateValue(cacheName, 0);
      assertAwaitEquals(null, cache.putIfAbsent(key, v0));
      kvGenerator.assertValueEquals(v0, await(cache.get(key)));

      final V v1 = kvGenerator.generateValue(cacheName, 1);
      CacheEntry<K, V> previousEntry = await(cache.putIfAbsent(key, v1));
      kvGenerator.assertKeyEquals(key, previousEntry.key());

      kvGenerator.assertValueEquals(v0, previousEntry.value());
      kvGenerator.assertValueEquals(v0, await(cache.get(key)));
      assertEntry(key, v0, kvGenerator, await(cache.getEntry(key)));
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testSetIfAbsent[{0}]")
   public void testSetIfAbsent(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V value = kvGenerator.generateValue(cacheName, 0);
      assertAwaitEquals(true, cache.setIfAbsent(key, value));
      kvGenerator.assertValueEquals(value, await(cache.get(key)));

      final V other = kvGenerator.generateValue(cacheName, 1);
      assertAwaitEquals(false, cache.setIfAbsent(key, other));

      final V actual = await(cache.get(key));
      kvGenerator.assertValueEquals(value, actual);

      assertEntry(key, value, kvGenerator, await(cache.getEntry(key)));
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "setIfAbsent[{0}]")
   public void testSet(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V v0 = kvGenerator.generateValue(cacheName, 0);
      await(cache.set(key, v0));
      kvGenerator.assertValueEquals(v0, await(cache.get(key)));

      final V v1 = kvGenerator.generateValue(cacheName, 1);
      await(cache.set(key, v1));
      V actual = await(cache.get(key));
      kvGenerator.assertValueEquals(v1, actual);

      assertEntry(key, v1, kvGenerator, await(cache.getEntry(key)));
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testGetAndRemove[{0}]")
   public void testGetAndRemove(KeyValueGenerator<K, V> kvGenerator) {
      final K key = kvGenerator.generateKey(cacheName, 0);
      final V value = kvGenerator.generateValue(cacheName, 0);
      assertEntry(key, null, kvGenerator, await(cache.put(key, value)));

      kvGenerator.assertValueEquals(value, await(cache.get(key)));
      assertEntry(key, value, kvGenerator, await(cache.getEntry(key)));
      assertEntry(key, value, kvGenerator, await(cache.getAndRemove(key)));

      assertAwaitEquals(null, cache.get(key));
   }

   @MethodSource("parameterized")
   @ParameterizedTest(name = "testPutAllAndClear[{0}]")
   public void testPutAll(KeyValueGenerator<K, V> kvGenerator) {
      Map<K, V> entries = new HashMap<>();
      for (int i = 0; i < 10; i++) {
         final K key = kvGenerator.generateKey(cacheName, i);
         final V value = kvGenerator.generateValue(cacheName, i);
         entries.put(key, value);
      }

      await(cache.putAll(entries));

      for (Map.Entry<K, V> entry : entries.entrySet()) {
         kvGenerator.assertValueEquals(entry.getValue(), await(cache.get(entry.getKey())));
      }

      await(cache.clear());
      for (Map.Entry<K, V> entry : entries.entrySet()) {
         assertAwaitEquals(null, cache.get(entry.getKey()));
      }
   }

   private void assertEntry(K key, V value, KeyValueGenerator<K, V> kv, CacheEntry<K, V> entry) {
      kv.assertKeyEquals(key, entry.key());
      kv.assertValueEquals(value, entry.value());
   }

   public Stream<Arguments> parameterized() {
      return Stream.of(
            Arguments.of(KeyValueGenerator.BYTE_ARRAY_GENERATOR),
            Arguments.of(KeyValueGenerator.STRING_GENERATOR),
            Arguments.of(KeyValueGenerator.GENERIC_ARRAY_GENERATOR)
      );
   }

}
