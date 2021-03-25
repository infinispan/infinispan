package org.infinispan.client.hotrod.marshall;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.MarshallerRegistry;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.IdentityMarshaller;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
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
   private static final Object VALUE = new CustomValue("this is the value");

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

   @AutoProtoSchemaBuilder(
         includeClasses = CustomValue.class,
         schemaFileName = "custom-value.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.client.MarshallerPerCacheTest"
   )
   interface CtxInitializer extends SerializationContextInitializer {
      CtxInitializer INSTANCE = new CtxInitializerImpl();
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return CtxInitializer.INSTANCE;
   }

   static class CustomValue implements Serializable {
      private String field;

      @ProtoFactory
      public CustomValue(String field) {
         this.field = field;
      }

      @ProtoField(1)
      public String getField() {
         return field;
      }

      public void setField(String field) {
         this.field = field;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         CustomValue that = (CustomValue) o;
         return field.equals(that.field);
      }

      @Override
      public int hashCode() {
         return Objects.hash(field);
      }
   }

   static final class CustomValueMarshaller extends AbstractMarshaller {

      @Override
      protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
         String json;
         if (o instanceof CustomValue) {
            CustomValue customValue = (CustomValue) o;
            json = Json.object().set("field", customValue.getField()).toString();
         } else {
            json = Json.make(o).asString();
         }
         return ByteBufferImpl.create(json.getBytes(UTF_8));
      }

      @Override
      public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
         Json json = Json.read(new String(buf, UTF_8));
         if (json.has("field")) {
            return new CustomValue(json.at("field").asString());
         }
         return json.asString();
      }

      @Override
      public boolean isMarshallable(Object o) {
         return o instanceof CustomValue || o instanceof String;
      }

      @Override
      public MediaType mediaType() {
         return APPLICATION_JSON;
      }
   }

   @Override
   protected ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      ConfigurationBuilder builder = super.createHotRodClientConfigurationBuilder(host, serverPort);
      builder.security().addJavaSerialAllowList(".*CustomValue.*");
      // Configure RemoteCaches with different marshallers
      builder.remoteCache(CACHE_DEFAULT).marshaller(ProtoStreamMarshaller.class);
      builder.remoteCache(CACHE_TEXT).marshaller(new CustomValueMarshaller());
      builder.remoteCache(CACHE_JAVA_SERIALIZED).marshaller(JavaSerializationMarshaller.class);
      return builder;
   }

   @Test
   public void testMarshallerPerCache() throws IOException, InterruptedException {
      MarshallerRegistry marshallerRegistry = remoteCacheManager.getMarshallerRegistry();

      assertMarshallerUsed(CACHE_DEFAULT, marshallerRegistry.getMarshaller(APPLICATION_PROTOSTREAM));
      assertMarshallerUsed(CACHE_TEXT, new CustomValueMarshaller());
      assertMarshallerUsed(CACHE_JAVA_SERIALIZED, marshallerRegistry.getMarshaller(APPLICATION_SERIALIZED_OBJECT));
   }

   @Test
   public void testOverrideMarshallerAtRuntime() throws Exception {
      // RemoteCache 'CACHE_JAVA_SERIALIZED' is configured to use the java marshaller
      RemoteCache<String, Object> cache = remoteCacheManager.getCache(CACHE_JAVA_SERIALIZED);
      cache.put("KEY", VALUE);

      // Override the value marshaller at runtime
      RemoteCache<String, byte[]> rawValueCache = cache.withDataFormat(DataFormat.builder().valueMarshaller(IdentityMarshaller.INSTANCE).build());

      // Make sure the key marshaller stays the same as configured in the cache, but the value marshaller is replaced
      byte[] rawValue = rawValueCache.get("KEY");
      assertArrayEquals(new JavaSerializationMarshaller().objectToByteBuffer(VALUE), rawValue);
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
