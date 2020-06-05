package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS;
import static org.eclipse.jetty.http.HttpHeader.ACCEPT_ENCODING;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.dataconversion.Gzip.decompress;
import static org.infinispan.rest.JSONConstants.TYPE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.AssertJUnit.assertEquals;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import org.assertj.core.api.Assertions;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.DateUtils;
import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.ResponseHeader;
import org.infinispan.rest.TestClass;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.search.entity.Person;
import org.infinispan.server.core.dataconversion.JsonTranscoder;
import org.infinispan.server.core.dataconversion.XMLTranscoder;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Test(groups = "functional", testName = "rest.CacheResourceTest")
public class CacheResourceTest extends BaseCacheResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      super.defineCaches(cm);
      ConfigurationBuilder object = getDefaultCacheBuilder();
      object.encoding().key().mediaType(TEXT_PLAIN_TYPE);
      object.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);

      ConfigurationBuilder legacyStorageCache = getDefaultCacheBuilder();
      legacyStorageCache.encoding().key().mediaType("application/x-java-object;type=java.lang.String");

      cm.defineConfiguration("objectCache", object.build());
      cm.defineConfiguration("legacy", legacyStorageCache.build());
      cm.defineConfiguration("rest", getDefaultCacheBuilder().build());
   }

   static {
      System.setProperty("infinispan.server.rest.cors-allow", "http://infinispan.org");
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new CacheResourceTest().withSecurity(false),
            new CacheResourceTest().withSecurity(true),
      };
   }

   @Test
   public void testLegacyPredefinedCache() throws Exception {
      putStringValueInCache("rest", "k1", "v1");

      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "rest", "k1"))
            .send();

      assertThat(response).isOk();
   }

   @Test
   public void shouldReadWriteToLegacyCache() throws Exception {
      //given
      putStringValueInCache("legacy", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "legacy", "test"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType("text/plain");
      assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldConvertExistingSerializableObjectToJson() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("objectCache", "test".getBytes(), testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "objectCache", "test"))
            .header(HttpHeader.ACCEPT, "application/json")
            .send();

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType("application/json");
      assertThat(response).hasReturnedText("{\"" + TYPE + "\":\"" + TestClass.class.getName() + "\",\"name\":\"test\"}");
   }

   @Test
   public void shouldConvertExistingSerializableObjectToXml() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("objectCache", "test".getBytes(), testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "objectCache", "test"))
            .header(HttpHeader.ACCEPT, "application/xml")
            .send();

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType("application/xml");
      assertThat(response).hasReturnedText(
            "<?xml version=\"1.0\" ?><org.infinispan.rest.TestClass><name>test</name></org.infinispan.rest.TestClass>");
   }

   @Test
   public void shouldReadAsBinaryWithPojoCache() throws Exception {
      //given
      String cacheName = "pojoCache";
      String key = "test";
      TestClass value = new TestClass();
      value.setName("test");

      putValueInCache(cacheName, key, value);

      //when
      ContentResponse response = get(cacheName, key, APPLICATION_OCTET_STREAM_TYPE);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldReadTextWithPojoCache() throws Exception {
      //given
      String cacheName = "pojoCache";
      String key = "k1";
      String value = "v1";

      putValueInCache(cacheName, key, value);

      //when
      ContentResponse response = get(cacheName, key, TEXT_PLAIN_TYPE);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(TEXT_PLAIN_TYPE);
      assertThat(response).hasReturnedText(value);
   }

   @Test
   public void shouldReadByteArrayWithPojoCache() throws Exception {
      //given
      Cache cache = restServer().getCacheManager().getCache("pojoCache").getAdvancedCache()
            .withEncoding(IdentityEncoder.class);
      cache.put("k1", "v1".getBytes());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "pojoCache", "k1"))
            .header(HttpHeader.ACCEPT, APPLICATION_OCTET_STREAM_TYPE)
            .send();

      //then
      assertThat(response).hasReturnedBytes("v1".getBytes());
      assertThat(response).isOk();
      assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldReadAsJsonWithPojoCache() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("pojoCache", "test", testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "pojoCache", "test"))
            .header(HttpHeader.ACCEPT, APPLICATION_JSON_TYPE)
            .send();

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(APPLICATION_JSON_TYPE);
      assertThat(response).hasReturnedText("{\"" + TYPE + "\":\"org.infinispan.rest.TestClass\",\"name\":\"test\"}");
   }

   @Test
   public void shouldNegotiateFromPojoCacheWithoutAccept() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      String cacheName = "pojoCache";
      String key = "k1";

      putValueInCache(cacheName, key, testClass);

      //when
      ContentResponse response = get(cacheName, key, null);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(MediaType.TEXT_PLAIN_TYPE);
      assertThat(response).hasReturnedText(testClass.toString());
   }

   @Test
   public void shouldWriteTextContentWithPjoCache() throws Exception {
      //given
      putStringValueInCache("pojoCache", "key1", "data");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "pojoCache", "key1"))
            .header(HttpHeader.ACCEPT, TEXT_PLAIN_TYPE)
            .send();

      //then
      assertThat(response).isOk();
      assertThat(response).hasReturnedText("data");
      assertThat(response).hasContentType(TEXT_PLAIN_TYPE);
   }

   @Test
   public void shouldWriteOctetStreamToDefaultCache() throws Exception {
      //given
      putBinaryValueInCache("default", "keyA", "<hey>ho</hey>".getBytes(), MediaType.APPLICATION_OCTET_STREAM);
      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "default", "keyA"))
            .send();

      //then
      assertThat(response).isOk();
      assertThat(response).hasReturnedBytes("<hey>ho</hey>".getBytes());
      assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldIgnoreDisabledCaches() throws Exception {
      putStringValueInCache("default", "K", "V");
      String url = String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "default", "K");

      ContentResponse response = client.newRequest(url).send();
      assertThat(response).isOk();

      restServer().ignoreCache("default");
      response = client.newRequest(url).send();
      assertThat(response).isServiceUnavailable();

      restServer().unignoreCache("default");
      response = client.newRequest(url).send();
      assertThat(response).isOk();
   }

   @Test
   public void shouldDeleteExistingValueEvenWithoutMetadata() throws Exception {
      putValueInCache("default", "test".getBytes(), "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "default", "test"))
            .method(HttpMethod.DELETE)
            .send();

      //then
      assertThat(response).isOk();
      Assertions.assertThat(restServer().getCacheManager().getCache("default")).isEmpty();
   }

   @Test
   public void testCORSPreflight() throws Exception {
      putValueInCache("default", "key", "value");

      int port = restServer().getPort();
      ContentResponse preFlight = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", port, "default", "key"))
            .method(HttpMethod.OPTIONS)
            .header(HttpHeader.HOST, "localhost")
            .header(HttpHeader.ORIGIN, "http://localhost:" + port)
            .header("access-control-request-method", "GET")
            .send();


      assertThat(preFlight).isOk();
      assertThat(preFlight).hasNoContent();
      assertThat(preFlight).containsAllHeaders(ACCESS_CONTROL_ALLOW_ORIGIN.toString(), ACCESS_CONTROL_ALLOW_METHODS.toString(), ACCESS_CONTROL_ALLOW_HEADERS.toString());
      assertThat(preFlight).hasHeaderWithValues(ACCESS_CONTROL_ALLOW_HEADERS.toString(), (String[]) RequestHeader.toArray());
   }

   @Test
   public void testCorsGET() throws Exception {
      putStringValueInCache("default", "test", "test");

      //when
      int port = restServer().getPort();
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", port, "default", "test"))
            .header(HttpHeader.ORIGIN, "http://127.0.0.1:" + port)
            .send();

      assertThat(response).isOk();
      assertThat(response).containsAllHeaders("access-control-allow-origin");
      assertThat(response).hasHeaderWithValues(ACCESS_CONTROL_EXPOSE_HEADERS.toString(), (String[]) ResponseHeader.toArray());
   }

   @Test
   public void testCorsAllowedJVMProp() throws Exception {
      int port = restServer().getPort();
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches", port))
            .header(HttpHeader.ORIGIN, "http://infinispan.org")
            .send();

      assertThat(response).isOk();
      assertThat(response).containsAllHeaders("access-control-allow-origin");
   }

   @Test
   public void testCORSAllOrigins() throws Exception {
      RestServerHelper restServerHelper = null;
      try {
         RestServerConfigurationBuilder restBuilder = new RestServerConfigurationBuilder();
         restBuilder.cors().addNewRule().allowOrigins(new String[]{"*"});
         restBuilder.host("localhost").port(0);
         restServerHelper = RestServerHelper.defaultRestServer();

         RestServerConfiguration build = restBuilder.build();

         restServerHelper.withConfiguration(build).start("test");

         ContentResponse response = client
               .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServerHelper.getPort(), "default", "test"))
               .header(HttpHeader.ORIGIN, "http://host.example.com:5576")
               .send();

         assertThat(response).containsAllHeaders("access-control-allow-origin");
      } finally {
         if (restServerHelper != null) restServerHelper.stop();
      }
   }

   @Test
   public void testIfModifiedHeaderForCache() throws Exception {
      putStringValueInCache("expiration", "test", "test");

      String url = String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "expiration", "test");
      ContentResponse resp = client.newRequest(url).send();
      String dateLast = resp.getHeaders().get("Last-Modified");

      ContentResponse sameLastModAndIfModified = client.newRequest(url).header("If-Modified-Since", dateLast).send();
      assertThat(sameLastModAndIfModified).isNotModified();

      putStringValueInCache("expiration", "test", "test-new");
      ContentResponse lastmodAfterIfModified = client.newRequest(url).send();
      dateLast = lastmodAfterIfModified.getHeaders().get("Last-Modified");
      assertThat(lastmodAfterIfModified).isOk();

      ContentResponse lastmodBeforeIfModified = client.newRequest(url).header("If-Modified-Since", plus1Day(dateLast)).send();
      assertThat(lastmodBeforeIfModified).isNotModified();
   }

   private String plus1Day(String rfc1123Date) {
      ZonedDateTime plus = DateUtils.parseRFC1123(rfc1123Date).plus(1, ChronoUnit.DAYS);
      return DateUtils.toRFC1123(plus.toEpochSecond() * 1000);
   }

   @Test
   public void testCompression() throws Exception {
      String payload = getResourceAsString("person.proto", getClass().getClassLoader());
      putStringValueInCache("default", "k", payload);

      HttpClient uncompressingClient = createNewClient();
      try {
         uncompressingClient.start();
         uncompressingClient.getContentDecoderFactories().clear();

         ContentResponse response = uncompressingClient
               .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "default", "k"))
               .header(HttpHeader.ACCEPT, "text/plain")
               .send();

         assertThat(response).hasNoContentEncoding();
         assertThat(response).hasContentLength(payload.getBytes().length);
         client.getContentDecoderFactories().clear();

         response = uncompressingClient
               .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "default", "k"))
               .header(HttpHeader.ACCEPT, "text/plain")
               .header(ACCEPT_ENCODING, "gzip")
               .send();

         assertThat(response).hasGzipContentEncoding();
         assertEquals(decompress(response.getContent()), payload);
      } finally {
         uncompressingClient.stop();
      }
   }

   @Test
   public void testReplaceExistingObject() throws Exception {
      String initialJson = "{\"" + TYPE + "\":\"org.infinispan.rest.TestClass\",\"name\":\"test\"}";
      String changedJson = "{\"" + TYPE + "\":\"org.infinispan.rest.TestClass\",\"name\":\"test2\"}";

      ContentResponse response = writeJsonToCache("key", initialJson, "objectCache");
      assertThat(response).isOk();

      response = writeJsonToCache("key", changedJson, "objectCache");
      assertThat(response).isOk();

      response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "objectCache", "key"))
            .header(HttpHeader.ACCEPT, APPLICATION_JSON_TYPE)
            .send();

      JsonNode jsonNode = new ObjectMapper().readTree(response.getContentAsString());
      assertEquals(jsonNode.get("name").asText(), "test2");
   }

   private ContentResponse writeJsonToCache(String key, String json, String cacheName) throws Exception {
      return client.newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), cacheName, key))
            .content(new StringContentProvider(json))
            .header(HttpHeader.CONTENT_TYPE, APPLICATION_JSON_TYPE)
            .method(HttpMethod.PUT).send();
   }

   @Test
   public void testServerDeserialization() throws Exception {
      Object value = new Person();

      byte[] jsonMarshalled = (byte[]) new JsonTranscoder().transcode(value, APPLICATION_OBJECT, APPLICATION_JSON);
      byte[] xmlMarshalled = (byte[]) new XMLTranscoder().transcode(value, APPLICATION_OBJECT, APPLICATION_XML);
      byte[] javaMarshalled = new JavaSerializationMarshaller().objectToByteBuffer(value);

      String expectError = "Class '" + value.getClass().getName() + "' blocked by deserialization white list";

      ContentResponse jsonResponse = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "objectCache", "addr2"))
            .content(new BytesContentProvider(jsonMarshalled))
            .header(HttpHeader.CONTENT_TYPE, APPLICATION_JSON_TYPE)
            .method(HttpMethod.PUT)
            .send();

      assertThat(jsonResponse).isError();
      assertThat(jsonResponse).containsReturnedText(expectError);

      ContentResponse xmlResponse = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "objectCache", "addr3"))
            .content(new BytesContentProvider(xmlMarshalled))
            .header(HttpHeader.CONTENT_TYPE, APPLICATION_XML_TYPE)
            .method(HttpMethod.PUT)
            .send();

      assertThat(xmlResponse).isError();
      assertThat(xmlResponse).containsReturnedText(expectError);

      ContentResponse serializationResponse = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), "objectCache", "addr4"))
            .content(new BytesContentProvider(javaMarshalled))
            .header(HttpHeader.CONTENT_TYPE, APPLICATION_SERIALIZED_OBJECT_TYPE)
            .method(HttpMethod.PUT)
            .send();

      assertThat(serializationResponse).isError();
      assertThat(serializationResponse).containsReturnedText(expectError);

   }
}
