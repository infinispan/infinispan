package org.infinispan.it.compatibility;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for interoperability between REST and HotRod endpoints without using compatibility mode,
 * but relying on MediaType configuration and HTTP Headers.
 *
 * @since 9.2
 */
@Test(groups = {"functional", "smoke"}, testName = "it.compatibility.EndpointInteroperabilityTest")
public class EndpointInteroperabilityTest extends AbstractInfinispanTest {

   /**
    * Cache with no MediaType configuration, assumes K and V are application/octet-stream.
    */
   private static final String BYTES_CACHE_NAME = "bytesCache";

   /**
    * Cache that will hold marshalled entries, configured to use application/x-jboss-marshalling for K and V.
    */
   private static final String MARSHALLED_CACHE_NAME = "marshalledCache";

   /**
    * Cache configured for text/plain for both K and V.
    */
   private static final String STRING_CACHE_NAME = "stringsCaches";

   private RestServer restServer;
   private HotRodServer hotRodServer;
   private HttpClient restClient;
   private EmbeddedCacheManager cacheManager;

   private RemoteCache<byte[], byte[]> bytesRemoteCache;
   private RemoteCache<Object, Object> defaultMarshalledRemoteCache;
   private RemoteCache<String, String> stringRemoteCache;

   @BeforeClass
   protected void setup() throws Exception {
      cacheManager = TestCacheManagerFactory.createServerModeCacheManager();

      cacheManager.defineConfiguration(BYTES_CACHE_NAME, getBytesCacheConfiguration().build());
      cacheManager.defineConfiguration(MARSHALLED_CACHE_NAME, getMarshalledCacheConfiguration().build());
      cacheManager.defineConfiguration(STRING_CACHE_NAME, getStringsCacheConfiguration().build());

      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.port(findFreePort());
      restServer = new RestServer();
      restServer.start(builder.build(), cacheManager);
      restClient = new HttpClient();

      hotRodServer = startHotRodServer(cacheManager);

      bytesRemoteCache = createRemoteCacheManager(new NoOpMarshaller()).getCache(BYTES_CACHE_NAME);
      defaultMarshalledRemoteCache = createRemoteCacheManager(null).getCache(MARSHALLED_CACHE_NAME);
      stringRemoteCache = createRemoteCacheManager(new UTF8StringMarshaller()).getCache(STRING_CACHE_NAME);
   }

   private ConfigurationBuilder getBytesCacheConfiguration() {
      return new ConfigurationBuilder();
   }

   private ConfigurationBuilder getMarshalledCacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(APPLICATION_JBOSS_MARSHALLING_TYPE);
      builder.encoding().value().mediaType(APPLICATION_JBOSS_MARSHALLING_TYPE);
      return builder;
   }

   private ConfigurationBuilder getStringsCacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(TEXT_PLAIN_TYPE);
      builder.encoding().value().mediaType(TEXT_PLAIN_TYPE);
      return builder;
   }

   protected RemoteCacheManager createRemoteCacheManager(Marshaller marshaller) {
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
      // 'application/x-jboss-marshalling' for both K and V.
      String key = "string-1";
      byte[] value = {1, 2, 3};
      defaultMarshalledRemoteCache.put(key, value);
      assertEquals(defaultMarshalledRemoteCache.get(key), value);

      // Read via rest the unmarshalled content.
      Object bytesFromRest = new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key(key).accept(APPLICATION_OCTET_STREAM)
            .read();
      assertArrayEquals((byte[]) bytesFromRest, value);

      // Read via Rest the raw content, as it is stored
      Object rawFromRest = new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key(key).accept(APPLICATION_JBOSS_MARSHALLING)
            .read();

      assertArrayEquals((byte[]) rawFromRest, new GenericJBossMarshaller().objectToByteBuffer(value));

      // Write via rest raw bytes
      String otherKey = "string-2";
      byte[] otherValue = {0x4, 0x5, 0x6};
      new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key(otherKey).value(otherValue, APPLICATION_OCTET_STREAM)
            .write();

      // Read via Hot Rod
      assertEquals(defaultMarshalledRemoteCache.get(otherKey), otherValue);
   }

   @Test
   public void testIntegerKeysAndByteArrayValue() throws Exception {
      String integerKeyType = "application/x-java-object; type=java.lang.Integer";
      byte[] value = {12};
      byte[] otherValue = "random".getBytes("UTF-8");

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
      GenericJBossMarshaller marshaller = new GenericJBossMarshaller();

      Object key = 1.1f;
      Object value = 32.4d;
      byte[] valueMarshalled = marshaller.objectToByteBuffer(value);

      defaultMarshalledRemoteCache.put(key, value);
      assertEquals(defaultMarshalledRemoteCache.get(key), value);

      // Read via Rest the raw byte[] as marshalled by the client
      Object bytesFromRest = new RestRequest().cache(MARSHALLED_CACHE_NAME)
            .key(key.toString(), floatContentType)
            .accept(APPLICATION_JBOSS_MARSHALLING)
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
   public void testStringKeysAndStringValues() throws Exception {
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
            .accept(APPLICATION_JBOSS_MARSHALLING)
            .read();

      assertArrayEquals((byte[]) marshalledContent, new GenericJBossMarshaller().objectToByteBuffer(value));

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
   public void testByteArrayKeysAndByteArrayValues() throws Exception {
      // Write via Hot Rod the byte[] content directly
      byte[] key = new byte[]{0x13, 0x26};
      byte[] value = new byte[]{10, 20};


      bytesRemoteCache.put(key, value);
      assertArrayEquals(bytesRemoteCache.get(key), value);

      // Read via Rest
      Object bytesFromRest = new RestRequest().cache(BYTES_CACHE_NAME)
            .key("0x1326", APPLICATION_OCTET_STREAM.withParameter("encoding", "hex").toString())
            .accept(APPLICATION_OCTET_STREAM)
            .read();

      assertEquals(bytesFromRest, value);

      // Write via rest
      byte[] newKey = new byte[]{0, 0, 0, 1};

      // Write via rest
      new RestRequest().cache(BYTES_CACHE_NAME)
            .key("0x00000001", MediaType.APPLICATION_OCTET_STREAM.withParameter("encoding", "hex").toString())
            .value(value)
            .write();

      // Read via Hot Rod
      assertEquals(bytesRemoteCache.get(newKey), value);
   }

   String getEndpoint(String cache) {
      return String.format("http://localhost:%s/rest/%s", restServer.getPort(), cache);
   }

   private String asString(Object content) throws Exception {
      if (content instanceof byte[]) {
         return new String((byte[]) content, "UTF-8");
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

      public RestRequest cache(String cacheName) {
         this.cacheName = cacheName;
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

      public RestRequest accept(MediaType valueContentType) {
         this.accept = valueContentType;
         return this;
      }

      public void write() throws Exception {
         EntityEnclosingMethod post = new PostMethod(getEndpoint(this.cacheName) + "/" + this.key);
         if (this.keyContentType != null) {
            post.addRequestHeader("Key-Content-Type", this.keyContentType);
         }
         if (this.value instanceof byte[]) {
            String contentType = this.contentType == null ? APPLICATION_OCTET_STREAM_TYPE : this.contentType;
            post.setRequestEntity(new ByteArrayRequestEntity((byte[]) this.value, contentType));
         } else {
            String payload = this.value.toString();
            String contentType = this.contentType == null ? TEXT_PLAIN_TYPE : this.contentType;
            post.setRequestEntity(new StringRequestEntity(payload, contentType, UTF_8.toString()));
         }
         restClient.executeMethod(post);
         assertEquals(post.getStatusCode(), HttpStatus.SC_OK);
      }

      public Object read() throws IOException {
         HttpMethod get = new GetMethod(getEndpoint(this.cacheName) + "/" + this.key);
         if (this.accept != null) {
            get.setRequestHeader(ACCEPT, this.accept.toString());
         }
         if (keyContentType != null) {
            get.setRequestHeader("Key-Content-Type", this.keyContentType);
         }
         restClient.executeMethod(get);
         assertEquals(HttpStatus.SC_OK, get.getStatusCode());
         return get.getResponseBody();
      }
   }

   @AfterClass
   protected void teardown() {
      bytesRemoteCache.getRemoteCacheManager().stop();
      defaultMarshalledRemoteCache.getRemoteCacheManager().stop();
      stringRemoteCache.getRemoteCacheManager().stop();
      if (restServer != null) {
         try {
            restServer.stop();
         } catch (Exception ignored) {
         }
      }
      killCacheManagers(cacheManager);
      killServers(hotRodServer);
   }

}
