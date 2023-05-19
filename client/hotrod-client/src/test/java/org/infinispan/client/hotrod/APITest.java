package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.BYTE_ARRAY_GENERATOR;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.GENERIC_ARRAY_GENERATOR;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.STRING_GENERATOR;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.tx.util.KeyValueGenerator;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

/**
 * Tests various API methods of remote cache
 *
 * @author William Burns
 * @since 11.0
 */
@Test(groups = "functional", testName = "client.hotrod.APITest")
public class APITest<K, V> extends MultiHotRodServersTest {

   private static final int NR_NODES = 2;
   private static final String CACHE_NAME = "api-cache";

   private KeyValueGenerator<K, V> kvGenerator;
   private boolean useJavaSerialization;
   private ProtocolVersion protocolVersion;

   @Override
   public Object[] factory() {
      return Arrays.stream(ProtocolVersion.values())
            // Don't include auto as it is a duplicate
            .filter(pv -> !pv.name().equals("PROTOCOL_VERSION_AUTO"))
            .flatMap(pv ->
                  Stream.of(
                        new APITest<byte[], byte[]>().keyValueGenerator(BYTE_ARRAY_GENERATOR).protocolVersion(pv),
                        new APITest<String, String>().keyValueGenerator(STRING_GENERATOR).protocolVersion(pv),
                        new APITest<Object[], Object[]>().keyValueGenerator(GENERIC_ARRAY_GENERATOR).javaSerialization().protocolVersion(pv)
                  )
            ).toArray(Object[]::new);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder
         (HotRodServer server) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = super.createHotRodClientConfigurationBuilder(server);
      builder.version(protocolVersion);
      return builder;
   }

   public void testCompute(Method method) {
      RemoteCache<K, V> cache = remoteCache();
      final K key = kvGenerator.generateKey(method, 0);
      final V value = kvGenerator.generateValue(method, 0);

      BiFunction<K, V, V> sameValueFunction = (k, v) -> v;
      cache.put(key, value);

      kvGenerator.assertValueEquals(value, cache.compute(key, sameValueFunction));
      kvGenerator.assertValueEquals(value, cache.get(key));

      final V value1 = kvGenerator.generateValue(method, 1);
      BiFunction<K, V, V> differentValueFunction = (k, v) -> value1;

      kvGenerator.assertValueEquals(value1, cache.compute(key, differentValueFunction));
      kvGenerator.assertValueEquals(value1, cache.get(key));

      final K notPresentKey = kvGenerator.generateKey(method, 1);
      kvGenerator.assertValueEquals(value1, cache.compute(notPresentKey, differentValueFunction));
      kvGenerator.assertValueEquals(value1, cache.get(notPresentKey));

      BiFunction<K, V, V> mappingToNull = (k, v) -> null;
      assertNull("mapping to null returns null", cache.compute(key, mappingToNull));
      assertNull("the key is removed", cache.get(key));

      int cacheSizeBeforeNullValueCompute = cache.size();
      K nonExistantKey = kvGenerator.generateKey(method, 3);
      assertNull("mapping to null returns null", cache.compute(nonExistantKey, mappingToNull));
      assertNull("the key does not exist", cache.get(nonExistantKey));
      assertEquals(cacheSizeBeforeNullValueCompute, cache.size());

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      BiFunction<Object, Object, V> mappingToException = (k, v) -> {
         throw computeRaisedException;
      };
      expectException(TransportException.class, RuntimeException.class, "hi there", () -> cache.compute(key, mappingToException));
   }

   public void testComputeIfAbsentMethods(Method method) {
      RemoteCache<K, V> cache = remoteCache();

      final K targetKey = kvGenerator.generateKey(method, 0);

      V value = kvGenerator.generateValue(method, 1);
      assertNull(cache.computeIfAbsent(targetKey, ignore -> null));

      // Exception are only thrown when value not exists.
      expectException(TransportException.class, RuntimeException.class, "expected exception", () ->
            cache.computeIfAbsent(targetKey, ignore -> { throw new RuntimeException("expected exception"); }));

      kvGenerator.assertValueEquals(value, cache.computeIfAbsent(targetKey, ignore -> value));
      kvGenerator.assertValueEquals(value, cache.get(targetKey));
      kvGenerator.assertValueEquals(value, cache.computeIfAbsent(targetKey, ignore -> kvGenerator.generateValue(method, 2)));
      kvGenerator.assertValueEquals(value, cache.get(targetKey));

      K anotherKey = kvGenerator.generateKey(method, 1);
      V anotherValue = kvGenerator.generateValue(method, 3);
      kvGenerator.assertValueEquals(anotherValue, cache.computeIfAbsent(anotherKey, ignore -> anotherValue, 1, TimeUnit.MINUTES, 3, TimeUnit.MINUTES));
   }

   public void testComputeIfPresentMethods(Method method) {
      RemoteCache<K, V> cache = remoteCache();

      final K targetKey = kvGenerator.generateKey(method, 0);
      V value = kvGenerator.generateValue(method, 0);
      assertNull(cache.computeIfPresent(targetKey, (k, v) -> value));
      assertNull(cache.get(targetKey));
      assertNull(cache.put(targetKey, value));
      kvGenerator.assertValueEquals(value, cache.get(targetKey));

      V anotherValue = kvGenerator.generateValue(method, 1);
      kvGenerator.assertValueEquals(anotherValue, cache.computeIfPresent(targetKey, (k, v) -> anotherValue));
      kvGenerator.assertValueEquals(anotherValue, cache.get(targetKey));

      // Exception are only thrown if a value exists.
      expectException(TransportException.class, RuntimeException.class, "expected exception", () ->
            cache.computeIfPresent(targetKey, (k, v) -> { throw new RuntimeException("expected exception"); }));

      int beforeSize = cache.size();
      assertNull(cache.computeIfPresent(targetKey, (k, v) -> null));
      assertNull(cache.get(targetKey));
      assertEquals(beforeSize - 1, cache.size());
   }

   public void testMergeMethods(Method method) {
      RemoteCache<K, V> cache = remoteCache();

      final K targetKey = kvGenerator.generateKey(method, 0);
      V targetValue = kvGenerator.generateValue(method, 0);

      BiFunction<? super V, ? super V, ? extends V> remappingFunction = (value1, value2) ->
            kvGenerator.generateValue(method, 2);

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.merge(targetKey, targetValue, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.merge(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.merge(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS));

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.mergeAsync(targetKey, targetValue, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.mergeAsync(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.mergeAsync(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS));
   }

   public void testPut(Method method) {
      RemoteCache<K, V> cache = remoteCache();

      final K targetKey = kvGenerator.generateKey(method, 0);
      V targetValue = kvGenerator.generateValue(method, 0);

      assertNull(cache.put(targetKey, targetValue));

      kvGenerator.assertValueEquals(targetValue, cache.withFlags(Flag.FORCE_RETURN_VALUE).put(targetKey,
            kvGenerator.generateValue(method, 2)));
   }

   public void testPutIfAbsent(Method method) {
      RemoteCache<K, V> cache = remoteCache();

      final K targetKey = kvGenerator.generateKey(method, 0);
      V targetValue = kvGenerator.generateValue(method, 0);

      assertNull(cache.putIfAbsent(targetKey, targetValue));

      kvGenerator.assertValueEquals(targetValue, cache.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(targetKey,
            kvGenerator.generateValue(method, 2)));
   }

   public void testRemove(Method method) {
      RemoteCache<K, V> cache = remoteCache();

      final K targetKey = kvGenerator.generateKey(method, 0);
      V targetValue = kvGenerator.generateValue(method, 0);

      assertNull(cache.put(targetKey, targetValue));

      kvGenerator.assertValueEquals(targetValue, cache.withFlags(Flag.FORCE_RETURN_VALUE).remove(targetKey));
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "kv", "protocolVersion");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), kvGenerator.toString(), protocolVersion.getVersion());
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createHotRodServers(NR_NODES, new ConfigurationBuilder());
      defineInAll(CACHE_NAME, cacheBuilder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(
         String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = super
            .createHotRodClientConfigurationBuilder(host, serverPort);
      clientBuilder.forceReturnValues(false);
      if (useJavaSerialization) {
         clientBuilder.marshaller(new JavaSerializationMarshaller()).addJavaSerialAllowList("\\Q[\\ELjava.lang.Object;");
      }
      return clientBuilder;
   }

   private APITest<K, V> keyValueGenerator(KeyValueGenerator<K, V> kvGenerator) {
      this.kvGenerator = kvGenerator;
      return this;
   }

   public APITest<K, V> javaSerialization() {
      useJavaSerialization = true;
      return this;
   }

   public APITest<K, V> protocolVersion(ProtocolVersion protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
   }

   private RemoteCache<K, V> remoteCache() {
      return client(0).getCache(CACHE_NAME);
   }
}
