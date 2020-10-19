package org.infinispan.it.endpoints;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.marshall.IdentityMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for interoperability between REST and HotRod endpoints without using unmarshalled objects,
 * but relying on MediaType configuration, HTTP Headers and Hot Rod client DataFormat support.
 *
 * @since 9.2
 */
@Test(groups = {"functional", "smoke"}, testName = "it.endpoints.EndpointInteroperabilityTest")
public class EndpointInteroperabilityTest extends AbstractInfinispanTest {

   /**
    * Cache with no MediaType configuration, assumes K and V are application/unknown.
    */
   private static final String DEFAULT_CACHE_NAME = "defaultCache";

   /**
    * Cache that will hold marshalled entries, configured to use application/x-protostream for K and V.
    */
   private static final String MARSHALLED_CACHE_NAME = "marshalledCache";

   /**
    * Cache configured for text/plain for both K and V.
    */
   private static final String STRING_CACHE_NAME = "stringsCaches";

   private RestServer restServer;
   private HotRodServer hotRodServer;
   private RestClient restClient;
   private EmbeddedCacheManager cacheManager;

   private RemoteCache<byte[], byte[]> defaultRemoteCache;
   private RemoteCache<Object, Object> defaultMarshalledRemoteCache;
   private RemoteCache<String, String> stringRemoteCache;

   @BeforeClass
   protected void setup() throws Exception {
      cacheManager = TestCacheManagerFactory.createServerModeCacheManager();

      cacheManager.defineConfiguration(DEFAULT_CACHE_NAME, getDefaultCacheConfiguration().build());
      cacheManager.defineConfiguration(MARSHALLED_CACHE_NAME, getMarshalledCacheConfiguration().build());
      cacheManager.defineConfiguration(STRING_CACHE_NAME, getStringsCacheConfiguration().build());

      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.port(findFreePort());
      restServer = new RestServer();
      restServer.start(builder.build(), cacheManager);
      RestClientConfigurationBuilder clientBuilder = new RestClientConfigurationBuilder();
      RestClientConfiguration configuration = clientBuilder.addServer().host(restServer.getHost()).port(restServer.getPort()).build();
      restClient = RestClient.forConfiguration(configuration);

      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      hotRodServer = startHotRodServer(cacheManager, serverBuilder);

      defaultRemoteCache = createRemoteCacheManager(IdentityMarshaller.INSTANCE).getCache(DEFAULT_CACHE_NAME);
      defaultMarshalledRemoteCache = createRemoteCacheManager(null).getCache(MARSHALLED_CACHE_NAME);
      stringRemoteCache = createRemoteCacheManager(new UTF8StringMarshaller()).getCache(STRING_CACHE_NAME);
   }

   private ConfigurationBuilder getDefaultCacheConfiguration() {
      return new ConfigurationBuilder();
   }

   private ConfigurationBuilder getMarshalledCacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      builder.encoding().value().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      return builder;
   }

   private ConfigurationBuilder getStringsCacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(TEXT_PLAIN_TYPE);
      builder.encoding().value().mediaType(TEXT_PLAIN_TYPE);
      return builder;
   }

   private RemoteCacheManager createRemoteCacheManager(Marshaller marshaller) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotRodServer.getPort());
      if (marshaller != null) {
         builder.marshaller(marshaller);
      }
      return new RemoteCacheManager(builder.build());
   }

   @Test
   public void testStringKeysAndByteArrayValue() throws Exception {
      // The Hot Rod client writes marshalled content. The cache is explicitly configured with
      // 'application/x-protostream' for both K and V.
      String key = "string-1";
      byte[] value = {1, 2, 3};
      byte[] marshalledValue = new ProtoStreamMarshaller().objectToByteBuffer(value);

      defaultMarshalledRemoteCache.put(key, value);
      assertEquals(defaultMarshalledRemoteCache.get(key), value);

      // Read via Rest the raw content, as it is stored
      Object rawFromRest = new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key(key).accept(APPLICATION_PROTOSTREAM)
            .read();

      assertArrayEquals((byte[]) rawFromRest, marshalledValue);

      // Write via rest raw bytes
      String otherKey = "string-2";
      byte[] otherValue = {0x4, 0x5, 0x6};
      byte[] otherValueMarshalled = new ProtoStreamMarshaller().objectToByteBuffer(otherValue);

      new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key(otherKey).value(otherValue, APPLICATION_OCTET_STREAM)
            .write();

      // Read via Hot Rod
      assertEquals(defaultMarshalledRemoteCache.get(otherKey), otherValue);

      // Read via Hot Rod using a String key, and getting the raw value (as it is stored) back
      DataFormat format = DataFormat.builder()
            .keyType(TEXT_PLAIN)
            .valueType(APPLICATION_PROTOSTREAM).valueMarshaller(IdentityMarshaller.INSTANCE)
            .build();
      byte[] rawValue = (byte[]) defaultMarshalledRemoteCache.withDataFormat(format).get(otherKey);
      assertArrayEquals(otherValueMarshalled, rawValue);

      // Read via Hot Rod using a String key, and getting the original value back
      DataFormat.builder().keyType(TEXT_PLAIN).build();
      byte[] result = (byte[]) defaultMarshalledRemoteCache
            .withDataFormat(DataFormat.builder().keyType(TEXT_PLAIN).build()).get(otherKey);
      assertArrayEquals(otherValue, result);
   }

   @Test
   public void testIntegerKeysAndByteArrayValue() {
      String integerKeyType = "application/x-java-object; type=java.lang.Integer";
      byte[] value = {12};
      byte[] otherValue = "random".getBytes(UTF_8);

      // Write <Integer, byte[]> via Hot Rod (the HR client is configured with the default marshaller)
      defaultMarshalledRemoteCache.put(10, value);
      assertEquals(defaultMarshalledRemoteCache.get(10), value);

      // Read via Rest
      Object bytesFromRest = new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key("10", integerKeyType).accept(APPLICATION_OCTET_STREAM)
            .read();

      assertArrayEquals((byte[]) bytesFromRest, value);

      // Write via rest
      new RestRequest().cache(MARSHALLED_CACHE_NAME).key("20", integerKeyType).value(otherValue).write();

      // Read via Hot Rod
      assertEquals(defaultMarshalledRemoteCache.get(20), otherValue);
   }

   @Test
   public void testFloatKeysDoubleValues() throws Exception {
      String floatContentType = "application/x-java-object; type=java.lang.Float";
      String doubleContentType = "application/x-java-object; type=java.lang.Double";
      Marshaller marshaller = new ProtoStreamMarshaller();

      Object key = 1.1f;
      Object value = 32.4d;
      byte[] valueMarshalled = marshaller.objectToByteBuffer(value);

      defaultMarshalledRemoteCache.put(key, value);
      assertEquals(defaultMarshalledRemoteCache.get(key), value);

      // Read via Rest the raw byte[] as marshalled by the client
      Object bytesFromRest = new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key(key.toString(), floatContentType)
            .accept(APPLICATION_PROTOSTREAM)
            .read();

      assertArrayEquals((byte[]) bytesFromRest, valueMarshalled);

      // Read via Rest the value as String
      Object stringFromRest = new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key(key.toString(), floatContentType)
            .accept(TEXT_PLAIN)
            .read();
      assertArrayEquals("32.4".getBytes(), (byte[]) stringFromRest);

      // Write via rest
      Object otherKey = 2.2f;
      Object otherValue = 123.0d;

      new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key(otherKey.toString(), floatContentType)
            .value(otherValue.toString(), doubleContentType)
            .write();

      // Read via Hot Rod
      assertEquals(defaultMarshalledRemoteCache.get(otherKey), otherValue);
   }

   @Test
   public void testStringKeysAndStringValues() {
      // Write via Hot Rod (the HR client is configured with a String marshaller)
      stringRemoteCache.put("key", "Hello World");
      assertEquals(stringRemoteCache.get("key"), "Hello World");

      // Read via Rest
      Object bytesFromRest = new RestRequest().cache(STRING_CACHE_NAME).key("key").accept(TEXT_PLAIN).read();
      assertEquals(asString(bytesFromRest), "Hello World");

      // Write via rest
      new RestRequest().cache(STRING_CACHE_NAME).key("key2").value("Testing").write();

      // Read via Hot Rod
      assertEquals(stringRemoteCache.get("key2"), "Testing");

      // Get values as JSON from Hot Rod
      Object jsonString = stringRemoteCache.withDataFormat(DataFormat.builder()
            .valueType(APPLICATION_JSON).valueMarshaller(new UTF8StringMarshaller()).build())
            .get("key");
      assertEquals("\"Hello World\"", jsonString);

   }

   @Test
   public void testByteArrayKeysAndValuesWithMarshaller() throws Exception {
      // Write via Hot Rod using the default marshaller
      byte[] key = new byte[]{0x74, 0x18};
      byte[] value = new byte[]{0x10, 0x20};

      defaultMarshalledRemoteCache.put(key, value);
      assertArrayEquals((byte[]) defaultMarshalledRemoteCache.get(key), value);

      // Read via Rest
      Object bytesFromRest = new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key("0x7418", APPLICATION_OCTET_STREAM_TYPE)
            .accept(APPLICATION_OCTET_STREAM)
            .read();

      assertEquals(bytesFromRest, value);

      // Read marshalled content directly
      Object marshalledContent = new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key("0x7418", APPLICATION_OCTET_STREAM_TYPE)
            .accept(APPLICATION_PROTOSTREAM)
            .read();

      assertArrayEquals((byte[]) marshalledContent, new ProtoStreamMarshaller().objectToByteBuffer(value));

      // Write via rest
      byte[] newKey = new byte[]{0x23};

      // Write via rest
      new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key("0x23", APPLICATION_OCTET_STREAM_TYPE)
            .value(value)
            .write();

      // Read via Hot Rod
      assertEquals(defaultMarshalledRemoteCache.get(newKey), value);
   }

   @Test
   public void testByteArrayKeysAndByteArrayValues() {
      // Write via Hot Rod the byte[] content directly
      byte[] key = new byte[]{0x13, 0x26};
      byte[] value = new byte[]{10, 20};


      defaultRemoteCache.put(key, value);
      assertArrayEquals(defaultRemoteCache.get(key), value);

      // Read via Rest
      Object bytesFromRest = new RestRequest().cache(DEFAULT_CACHE_NAME)
            .key("0x1326", APPLICATION_OCTET_STREAM.withParameter("encoding", "hex").toString())
            .accept(APPLICATION_OCTET_STREAM)
            .read();

      assertEquals(bytesFromRest, value);

      // Write via rest
      byte[] newKey = new byte[]{0, 0, 0, 1};

      // Write via rest
      new RestRequest().cache(DEFAULT_CACHE_NAME)
            .key("0x00000001", APPLICATION_OCTET_STREAM.withParameter("encoding", "hex").toString())
            .value(value)
            .write();

      // Read via Hot Rod
      assertEquals(defaultRemoteCache.get(newKey), value);
   }

   @Test
   public void testCustomKeysAndByteValues() throws Exception {

      String customKeyType = "application/x-java-object; type=ByteArray";

      CustomKey objectKey = new CustomKey("a", 1.0d, 1.0f, true);
      ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();
      marshaller.register(EndpointITSCI.INSTANCE);
      byte[] key = marshaller.objectToByteBuffer(objectKey);
      byte[] value = {12};

      // Write <byte[], byte[]> via Hot Rod (the HR client is configured with a no-op marshaller)
      defaultRemoteCache.put(key, value);
      assertEquals(value, defaultRemoteCache.get(key));

      String restKey = StandardConversions.bytesToHex(key);

      // Read via Rest
      Object bytesFromRest = new RestRequest().cache(DEFAULT_CACHE_NAME)
            .key(restKey, customKeyType).accept(APPLICATION_OCTET_STREAM)
            .read();

      assertArrayEquals((byte[]) bytesFromRest, value);
   }

   @Test
   public void testCacheLifecycle() {
      // Write from Hot Rod
      stringRemoteCache.put("key", "Hello World");
      assertEquals(stringRemoteCache.get("key"), "Hello World");

      // Read from REST
      RestRequest restRequest = new RestRequest().cache(STRING_CACHE_NAME).key("key").accept(TEXT_PLAIN);
      assertEquals(restRequest.executeGet().getBody(), "Hello World");

      // Delete the cache
      RemoteCacheManagerAdmin admin = stringRemoteCache.getRemoteCacheManager().administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE);
      admin.removeCache(stringRemoteCache.getName());

      // Check cache not available
      assertClientError(() -> stringRemoteCache.get("key"), "CacheNotFoundException");
      assertEquals(restRequest.executeGet().getStatus(), 404);

      // Recreate the cache
      RemoteCache<String, String> recreated = admin.getOrCreateCache(stringRemoteCache.getName(), new ConfigurationBuilder()
            .encoding().key().mediaType(TEXT_PLAIN_TYPE)
            .encoding().value().mediaType(TEXT_PLAIN_TYPE)
            .build());

      // Write from Hot Rod
      recreated.put("key", "Hello World");
      assertEquals(recreated.get("key"), "Hello World");

      // Read from REST
      assertEquals(restRequest.executeGet().getBody(), "Hello World");
   }

   private void assertClientError(Runnable runnable, String messagePart) {
      try {
         runnable.run();
      } catch (Throwable t) {
         String message = t.getMessage();
         assertTrue(message != null && message.contains(messagePart));
      }
   }

   private String asString(Object content) {
      if (content instanceof byte[]) {
         return new String((byte[]) content, UTF_8);
      }
      return content.toString();
   }

   private class RestRequest {
      private String cacheName;
      private Object key;
      private Object value;
      private String keyContentType;
      private MediaType accept;
      private String contentType;
      private RestCacheClient restCacheClient;

      public RestRequest cache(String cacheName) {
         this.cacheName = cacheName;
         this.restCacheClient = restClient.cache(cacheName);
         return this;
      }

      public RestRequest key(Object key) {
         this.key = key;
         return this;
      }

      public RestRequest key(Object key, String keyContentType) {
         this.key = key;
         this.keyContentType = keyContentType;
         return this;
      }

      public RestRequest value(Object value, String contentType) {
         this.value = value;
         this.contentType = contentType;
         return this;
      }

      public RestRequest value(Object value, MediaType contentType) {
         this.value = value;
         this.contentType = contentType.toString();
         return this;
      }

      public RestRequest value(Object value) {
         this.value = value;
         return this;
      }

      RestRequest accept(MediaType valueContentType) {
         this.accept = valueContentType;
         return this;
      }

      void write() {
         RestEntity restEntity;
         if (this.value instanceof byte[]) {
            MediaType contentType = this.contentType == null ? APPLICATION_OCTET_STREAM : MediaType.fromString(this.contentType);
            restEntity = RestEntity.create(contentType, (byte[]) this.value);
         } else {
            String payload = this.value.toString();
            MediaType contentType = this.contentType == null ? TEXT_PLAIN : MediaType.fromString(this.contentType);
            restEntity = RestEntity.create(contentType, payload);
         }
         RestResponse response;
         if (this.keyContentType != null) {
            response = join(restCacheClient.put(this.key.toString(), keyContentType, restEntity));
         } else {
            response = join(restCacheClient.put(this.key.toString(), restEntity));
         }
         assertEquals(204, response.getStatus());
      }

      RestResponse executeGet() {
         Map<String, String> headers = new HashMap<>();
         if (this.accept != null) {
            headers.put(RequestHeader.ACCEPT_HEADER.getValue(), this.accept.toString());
         }
         if (keyContentType != null) {
            headers.put(RequestHeader.KEY_CONTENT_TYPE_HEADER.getValue(), this.keyContentType);
         }
         return join(restCacheClient.get(this.key.toString(), headers));
      }

      Object read() {
         RestResponse response = executeGet();
         assertEquals(response.getStatus(), 200);
         return response.getBodyAsByteArray();
      }
   }

   @AfterClass
   protected void teardown() {
      defaultRemoteCache.getRemoteCacheManager().stop();
      defaultMarshalledRemoteCache.getRemoteCacheManager().stop();
      stringRemoteCache.getRemoteCacheManager().stop();
      if (restServer != null) {
         try {
            restClient.close();
            restServer.stop();
         } catch (Exception ignored) {
         }
      }
      killCacheManagers(cacheManager);
      cacheManager = null;
      killServers(hotRodServer);
      hotRodServer = null;
   }

}
