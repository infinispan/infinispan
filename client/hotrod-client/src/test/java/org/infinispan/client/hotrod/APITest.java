package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.BYTE_ARRAY_GENERATOR;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.GENERIC_ARRAY_GENERATOR;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.STRING_GENERATOR;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.tx.util.KeyValueGenerator;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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

   @Override
   public Object[] factory() {
      return new Object[]{
            new APITest<byte[], byte[]>().keyValueGenerator(BYTE_ARRAY_GENERATOR),
            new APITest<String, String>().keyValueGenerator(STRING_GENERATOR),
            new APITest<Object[], Object[]>().keyValueGenerator(GENERIC_ARRAY_GENERATOR).javaSerialization(),
      };
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

      Function<? super K, ? extends V> remappingFunction = key ->
            kvGenerator.generateValue(method, 1);

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfAbsent(targetKey, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfAbsent(targetKey, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfAbsent(targetKey, remappingFunction, 1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS));

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfAbsentAsync(targetKey, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfAbsentAsync(targetKey, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfAbsentAsync(targetKey, remappingFunction, 1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS));
   }

   public void testComputeIfPresentMethods(Method method) {
      RemoteCache<K, V> cache = remoteCache();

      final K targetKey = kvGenerator.generateKey(method, 0);

      BiFunction<? super K, ? super V, ? extends V> remappingFunction = (key, value) ->
            kvGenerator.generateValue(method, 1);

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfPresent(targetKey, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfPresent(targetKey, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfPresent(targetKey, remappingFunction, 1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS));

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfPresentAsync(targetKey, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfPresentAsync(targetKey, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.computeIfPresentAsync(targetKey, remappingFunction, 1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS));
   }

   public void testMergeMethods(Method method) {
      RemoteCache<K, V> cache = remoteCache();

      final K targetKey = kvGenerator.generateKey(method, 0);
      V targetValue = kvGenerator.generateValue(method, 0);

      BiFunction<? super V, ? super V, ? extends V> remappingFunction = (value1, value2) ->
            kvGenerator.generateValue(method, 2);

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.merge(targetKey, targetValue, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.merge(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.merge(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS,  10, TimeUnit.SECONDS));

      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.mergeAsync(targetKey, targetValue, remappingFunction));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.mergeAsync(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS));
      Exceptions.expectException(UnsupportedOperationException.class, () -> cache.mergeAsync(targetKey, targetValue, remappingFunction, 1, TimeUnit.SECONDS,  10, TimeUnit.SECONDS));
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), null);
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), kvGenerator.toString());
   }

   @Override
   protected String parameters() {
      return "[" + kvGenerator + "]";
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

   private RemoteCache<K, V> remoteCache() {
      return client(0).getCache(CACHE_NAME);
   }
}
