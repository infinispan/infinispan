package org.infinispan.client.hotrod.marshall;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.IOException;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.marshall.IdentityMarshaller;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @since 12.1
 */
@Test(groups = "functional", testName = "client.hotrod.MarshallerPerCacheTest")
public class MarshallerPerCacheTest extends SingleHotRodServerTest {

   private static final String CACHE_TEXT = "text";
   private static final String CACHE_JAVA_SERIALIZED = "serialized";
   private static final String CACHE_DEFAULT = "default";

   private static final Object KEY = 1;
   private static final Object VALUE = "this is the value";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gcb, hotRodCacheConfiguration());
      Configuration protobufConfig = new org.infinispan.configuration.cache.ConfigurationBuilder()
            .encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE)
            .build();
      Configuration defaultConfig = new org.infinispan.configuration.cache.ConfigurationBuilder().build();
      cm.createCache(CACHE_TEXT, defaultConfig);
      cm.createCache(CACHE_JAVA_SERIALIZED, defaultConfig);
      cm.createCache(CACHE_DEFAULT, protobufConfig);
      return cm;
   }

   @Override
   protected ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      ConfigurationBuilder builder = super.createHotRodClientConfigurationBuilder(host, serverPort);

      // Configure RemoteCaches with different marshallers
      builder.remoteCache(CACHE_TEXT).marshaller(UTF8StringMarshaller.class);
      builder.remoteCache(CACHE_JAVA_SERIALIZED).marshaller(JavaSerializationMarshaller.class);
      return builder;
   }

   @Test
   public void testMarshallerPerCache() throws IOException, InterruptedException {
      assertMarshallerUsed(CACHE_DEFAULT, new ProtoStreamMarshaller());
      assertMarshallerUsed(CACHE_TEXT, new UTF8StringMarshaller());
      assertMarshallerUsed(CACHE_JAVA_SERIALIZED, new JavaSerializationMarshaller());
   }

   @Test
   public void testOverrideMarshallerAtRuntime() throws Exception {
      // RemoteCache 'CACHE_JAVA_SERIALIZED' is configured to use the java marshaller
      RemoteCache<String, String> cache = remoteCacheManager.getCache(CACHE_JAVA_SERIALIZED);
      cache.put("KEY", "VALUE");

      // Override the value marshaller at runtime
      RemoteCache<String, byte[]> rawValueCache = cache.withDataFormat(DataFormat.builder().valueMarshaller(IdentityMarshaller.INSTANCE).build());

      // Make sure the key marshaller stays the same as configured in the cache, but the value marshaller is replaced
      byte[] rawValue = rawValueCache.get("KEY");
      assertArrayEquals(new JavaSerializationMarshaller().objectToByteBuffer("VALUE"), rawValue);
   }

   private void assertMarshallerUsed(String cacheName, Marshaller expectedMarshaller) throws InterruptedException, IOException {
      // Read and write to the remote cache
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache(cacheName);
      remoteCache.put(KEY, VALUE);
      assertEquals(VALUE, remoteCache.get(KEY));

      // Read data from the embedded cache as it is stored
      Cache<byte[], byte[]> cache = cacheManager.getCache(remoteCache.getName());
      AdvancedCache<byte[], byte[]> asStored = cache.getAdvancedCache().withStorageMediaType();

      // Assert that data was written using the 'expectedMarshaller'
      byte[] keyMarshalled = expectedMarshaller.objectToByteBuffer(KEY);
      byte[] valueMarshalled = expectedMarshaller.objectToByteBuffer(VALUE);
      assertArrayEquals(valueMarshalled, asStored.get(keyMarshalled));
   }
}
