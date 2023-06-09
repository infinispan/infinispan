package org.infinispan.server.functional.hotrod;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodCacheOperations<K, V> {

   private static final String TEST_OUTPUT = "{0}-{1}";

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;
   static class ArgsProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
         return Arrays.stream(ProtocolVersion.values())
               .flatMap(version ->
                     Stream.of(
                           Arguments.of(version, KeyValueGenerator.STRING_GENERATOR),
                           Arguments.of(version, KeyValueGenerator.BYTE_ARRAY_GENERATOR),
                           Arguments.of(version, KeyValueGenerator.GENERIC_ARRAY_GENERATOR)
                     )
               );
      }
   }

   private RemoteCache<K, V> remoteCache(ProtocolVersion protocolVersion, boolean frv) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.version(protocolVersion).forceReturnValues(frv);
      return SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
   }

   private RemoteCache<K, V> remoteCache(ProtocolVersion protocolVersion) {
      return remoteCache(protocolVersion, false);
   }

   @ParameterizedTest(name = TEST_OUTPUT)
   @ArgumentsSource(ArgsProvider.class)
   public void testCompute(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) {
      RemoteCache<K, V> cache = remoteCache(protocolVersion);
      final K key = generator.key(0);
      final V value = generator.value(0);

      BiFunction<K, V, V> sameValueFunction = (k, v) -> v;
      cache.put(key, value);

      generator.assertEquals(value, cache.compute(key, sameValueFunction));
      generator.assertEquals(value, cache.get(key));

      final V value1 = generator.value(1);
      BiFunction<K, V, V> differentValueFunction = (k, v) -> value1;

      generator.assertEquals(value1, cache.compute(key, differentValueFunction));
      generator.assertEquals(value1, cache.get(key));

      final K notPresentKey = generator.key(1);
      generator.assertEquals(value1, cache.compute(notPresentKey, differentValueFunction));
      generator.assertEquals(value1, cache.get(notPresentKey));

      BiFunction<K, V, V> mappingToNull = (k, v) -> null;
      assertNull(cache.compute(key, mappingToNull), "mapping to null returns null");
      assertNull(cache.get(key), "the key is removed");

      int cacheSizeBeforeNullValueCompute = cache.size();
      K nonExistantKey = generator.key(3);
      assertNull(cache.compute(nonExistantKey, mappingToNull), "mapping to null returns null");
      assertNull(cache.get(nonExistantKey), "the key does not exist");
      assertEquals(cacheSizeBeforeNullValueCompute, cache.size());

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      BiFunction<Object, Object, V> mappingToException = (k, v) -> {
         throw computeRaisedException;
      };
      expectException(TransportException.class, RuntimeException.class, "hi there", () -> cache.compute(key, mappingToException));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testComputeIfAbsentMethods(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) {
      RemoteCache<K, V> cache = remoteCache(protocolVersion);

      final K targetKey = generator.key(0);

      V value = generator.value(1);
      assertNull(cache.computeIfAbsent(targetKey, ignore -> null));

      // Exception are only thrown when value not exists.
      expectException(TransportException.class, RuntimeException.class, "expected exception", () ->
            cache.computeIfAbsent(targetKey, ignore -> { throw new RuntimeException("expected exception"); }));

      generator.assertEquals(value, cache.computeIfAbsent(targetKey, ignore -> value));
      generator.assertEquals(value, cache.get(targetKey));
      generator.assertEquals(value, cache.computeIfAbsent(targetKey, ignore -> generator.value(2)));
      generator.assertEquals(value, cache.get(targetKey));

      K anotherKey = generator.key(1);
      V anotherValue = generator.value(3);
      generator.assertEquals(anotherValue, cache.computeIfAbsent(anotherKey, ignore -> anotherValue, 1, TimeUnit.MINUTES, 3, TimeUnit.MINUTES));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testComputeIfPresentMethods(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) {
      RemoteCache<K, V> cache = remoteCache(protocolVersion);

      final K targetKey = generator.key(0);
      V value = generator.value(0);
      assertNull(cache.computeIfPresent(targetKey, (k, v) -> value));
      assertNull(cache.get(targetKey));
      assertNull(cache.put(targetKey, value));
      generator.assertEquals(value, cache.get(targetKey));

      V anotherValue = generator.value(1);
      generator.assertEquals(anotherValue, cache.computeIfPresent(targetKey, (k, v) -> anotherValue));
      generator.assertEquals(anotherValue, cache.get(targetKey));

      // Exception are only thrown if a value exists.
      expectException(TransportException.class, RuntimeException.class, "expected exception", () ->
            cache.computeIfPresent(targetKey, (k, v) -> { throw new RuntimeException("expected exception"); }));

      int beforeSize = cache.size();
      assertNull(cache.computeIfPresent(targetKey, (k, v) -> null));
      assertNull(cache.get(targetKey));
      assertEquals(beforeSize - 1, cache.size());
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testMergeMethods(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) {
      RemoteCache<K, V> cache = remoteCache(protocolVersion);

      final K targetKey = generator.key(0);
      V targetValue = generator.value(0);

      BiFunction<? super V, ? super V, ? extends V> remappingFunction = (value1, value2) ->
            generator.value(2);

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.merge(targetKey, targetValue, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.merge(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.merge(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS));

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.mergeAsync(targetKey, targetValue, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.mergeAsync(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.mergeAsync(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testPut(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) {
      RemoteCache<K, V> cache = remoteCache(protocolVersion);

      final K targetKey = generator.key(0);
      V targetValue = generator.value(0);

      assertNull(cache.put(targetKey, targetValue));

      generator.assertEquals(targetValue, cache.withFlags(Flag.FORCE_RETURN_VALUE).put(targetKey,
            generator.value(2)));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testPutIfAbsent(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) {
      RemoteCache<K, V> cache = remoteCache(protocolVersion);
      final K targetKey = generator.key(0);
      V targetValue = generator.value(0);

      assertNull(cache.putIfAbsent(targetKey, targetValue));

      generator.assertEquals(targetValue, cache.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(targetKey,
            generator.value(2)));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testRemove(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) {
      RemoteCache<K, V> cache = remoteCache(protocolVersion);
      final K targetKey = generator.key(0);
      V targetValue = generator.value(0);

      assertNull(cache.put(targetKey, targetValue));

      generator.assertEquals(targetValue, cache.withFlags(Flag.FORCE_RETURN_VALUE).remove(targetKey));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testPutAsync(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v = generator.value(0);
      final V v2 = generator.value(2);
      Future<V> f = cache.putAsync(k, v);
      testFuture(generator, f, null);
      generator.assertEquals(v, cache.get(k));
      f = cache.putAsync(k, v2);
      testFuture(generator, f, v);
      generator.assertEquals(v2, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testPutAsyncWithListener(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v = generator.value(0);
      final V v2 = generator.value(2);
      CompletableFuture<V> f = cache.putAsync(k, v);
      testFutureWithListener(generator, f, null);
      generator.assertEquals(v, cache.get(k));

      f = cache.putAsync(k, v2);
      testFutureWithListener(generator, f, v);
      generator.assertEquals(v2, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testPutAllAsync(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v3 = generator.value(3);
      Future<Void> f = cache.putAllAsync(Collections.singletonMap(k, v3));
      assertNull(f.get());
      generator.assertEquals(v3, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testPutAllAsyncWithListener(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v3 = generator.value(3);
      CompletableFuture<Void> f = cache.putAllAsync(Collections.singletonMap(k, v3));
      testFutureWithListener(f);
      generator.assertEquals(v3, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testPutIfAbsentAsync(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v3 = generator.value(3);
      final V v4 = generator.value(4);
      final V v5 = generator.value(5);
      cache.put(k, v3);
      generator.assertEquals(v3, cache.get(k));

      Future<V> f = cache.putIfAbsentAsync(k, v4);
      testFuture(generator, f, v3);
      generator.assertEquals(v3, cache.remove(k));

      f = cache.putIfAbsentAsync(k, v5);
      testFuture(generator, f, null);
      generator.assertEquals(v5, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testPutIfAbsentAsyncWithListener(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v3 = generator.value(3);
      final V v4 = generator.value(4);
      final V v5 = generator.value(5);
      cache.put(k, v3);
      generator.assertEquals(v3, cache.get(k));

      CompletableFuture<V> f = cache.putIfAbsentAsync(k, v4);
      testFutureWithListener(generator, f, v3);
      generator.assertEquals(v3, cache.remove(k));

      f = cache.putIfAbsentAsync(k, v5);
      testFutureWithListener(generator, f, null);
      generator.assertEquals(v5, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testRemoveAsync(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v3 = generator.value(3);
      cache.put(k, v3);
      generator.assertEquals(v3, cache.get(k));

      Future<V> f = cache.removeAsync(k);
      testFuture(generator, f, v3);
      assertNull(cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testRemoveAsyncWithListener(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v3 = generator.value(3);
      cache.put(k, v3);
      generator.assertEquals(v3, cache.get(k));

      CompletableFuture<V> f = cache.removeAsync(k);
      testFutureWithListener(generator, f, v3);
      assertNull(cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testGetAsync(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v = generator.value(0);
      cache.put(k, v);
      generator.assertEquals(v, cache.get(k));

      Future<V> f = cache.getAsync(k);
      testFuture(generator, f, v);
      generator.assertEquals(v, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testGetAsyncWithListener(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v = generator.value(0);
      cache.put(k, v);
      generator.assertEquals(v, cache.get(k));

      CompletableFuture<V> f = cache.getAsync(k);
      testFutureWithListener(generator, f, v);
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testRemoveWithVersionAsync(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v4 = generator.value(4);
      cache.put(k, v4);
      VersionedValue<V> value = cache.getWithMetadata(k);

      Future<Boolean> f = cache.removeWithVersionAsync(k, value.getVersion() + 1);
      assertFalse(f.get());
      generator.assertEquals(v4, cache.get(k));

      f = cache.removeWithVersionAsync(k, value.getVersion());
      assertTrue(f.get());
      assertNull(cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testRemoveWithVersionAsyncWithListener(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      final K k = generator.key(0);
      final V v4 = generator.value(4);
      cache.put(k, v4);
      VersionedValue<V> value = cache.getWithMetadata(k);

      CompletableFuture<Boolean> f = cache.removeWithVersionAsync(k, value.getVersion() + 1);
      testFutureWithListener(f, false);
      generator.assertEquals(v4, cache.get(k));

      f = cache.removeWithVersionAsync(k, value.getVersion());
      testFutureWithListener(f, true);
      assertNull(cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testReplaceAsync(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      K k = generator.key(0);
      V v = generator.value(0);
      V v5 = generator.value(5);
      assertNull(cache.get(k));
      Future<V> f = cache.replaceAsync(k, v5);
      testFuture(generator, f, null);
      assertNull(cache.get(k));

      cache.put(k, v);
      generator.assertEquals(v, cache.get(k));
      f = cache.replaceAsync(k, v5);
      testFuture(generator, f, v);
      generator.assertEquals(v5, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testReplaceAsyncWithListener(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      K k = generator.key(0);
      V v = generator.value(0);
      V v5 = generator.value(5);
      assertNull(cache.get(k));
      CompletableFuture<V> f = cache.replaceAsync(k, v5);
      testFutureWithListener(generator, f, null);
      assertNull(cache.get(k));

      cache.put(k, v);
      generator.assertEquals(v, cache.get(k));
      f = cache.replaceAsync(k, v5);
      testFutureWithListener(generator, f, v);
      generator.assertEquals(v5, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testReplaceWithVersionAsync(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      K k = generator.key(0);
      V v = generator.value(0);
      V v2 = generator.value(2);
      V v3 = generator.value(3);

      cache.put(k, v);
      VersionedValue<V> versioned1 = cache.getWithMetadata(k);

      Future<Boolean> f = cache.replaceWithVersionAsync(k, v2, versioned1.getVersion());
      assertTrue(f.get());

      VersionedValue<V> versioned2 = cache.getWithMetadata(k);
      assertNotEquals(versioned1.getVersion(), versioned2.getVersion());
      generator.assertEquals(versioned2.getValue(), v2);

      f = cache.replaceWithVersionAsync(k, v3, versioned1.getVersion());
      assertFalse(f.get());
      generator.assertEquals(v2, cache.get(k));
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testReplaceWithVersionAsyncWithListener(ProtocolVersion protocolVersion, KeyValueGenerator<K, V> generator) throws Exception {
      RemoteCache<K, V> cache = remoteCache(protocolVersion, true);
      K k = generator.key(0);
      V v = generator.value(0);
      V v2 = generator.value(2);
      V v3 = generator.value(3);
      cache.put(k, v);
      VersionedValue<V> versioned1 = cache.getWithMetadata(k);

      CompletableFuture<Boolean> f = cache.replaceWithVersionAsync(k, v2, versioned1.getVersion());
      testFutureWithListener(f, true);

      VersionedValue<V> versioned2 = cache.getWithMetadata(k);
      assertNotEquals(versioned1.getVersion(), versioned2.getVersion());
      generator.assertEquals(versioned2.getValue(), v2);

      f = cache.replaceWithVersionAsync(k, v3, versioned1.getVersion());
      testFutureWithListener(f, false);
      generator.assertEquals(v2, cache.get(k));
   }

   private void testFuture(KeyValueGenerator<K, V> generator, Future<V> f, V expected) throws ExecutionException, InterruptedException {
      assertNotNull(f);
      assertFalse(f.isCancelled());
      V value = f.get();
      generator.assertEquals(expected, value);
      assertTrue(f.isDone());
   }

   private void testFutureWithListener(KeyValueGenerator<K, V> generator, CompletableFuture<V> f, V expected) throws InterruptedException {
      assertNotNull(f);
      AtomicReference<Throwable> ex = new AtomicReference<>();
      CountDownLatch latch = new CountDownLatch(1);
      f.whenComplete((v, t) -> {
         if (t != null) {
            ex.set(t);
         }
         generator.assertEquals(expected, v);
         latch.countDown();
      });
      if (!latch.await(5, TimeUnit.SECONDS)) {
         fail("Not finished within 5 seconds");
      }
      if (ex.get() != null) {
         throw new AssertionError(ex.get());
      }
   }

   private void testFutureWithListener(CompletableFuture<Boolean> f, boolean expected) throws InterruptedException {
      assertNotNull(f);
      AtomicReference<Throwable> ex = new AtomicReference<>();
      CountDownLatch latch = new CountDownLatch(1);
      f.whenComplete((v, t) -> {
         if (t != null) {
            ex.set(t);
         }
         assertEquals(expected, v);
         latch.countDown();
      });
      if (!latch.await(5, TimeUnit.SECONDS)) {
         fail("Not finished within 5 seconds");
      }
      if (ex.get() != null) {
         throw new AssertionError(ex.get());
      }
   }

   private void testFutureWithListener(CompletableFuture<Void> f) throws InterruptedException {
      assertNotNull(f);
      AtomicReference<Throwable> ex = new AtomicReference<>();
      CountDownLatch latch = new CountDownLatch(1);
      f.whenComplete((v, t) -> {
         if (t != null) {
            ex.set(t);
         }
         assertNull(v);
         latch.countDown();
      });
      if (!latch.await(5, TimeUnit.SECONDS)) {
         fail("Not finished within 5 seconds");
      }
      if (ex.get() != null) {
         throw new AssertionError(ex.get());
      }
   }
}
