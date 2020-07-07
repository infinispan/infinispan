package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpHeaderNames.ORIGIN;
import static java.util.Collections.singletonMap;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.dataconversion.Gzip.decompress;
import static org.infinispan.rest.JSONConstants.TYPE;
import static org.infinispan.rest.RequestHeader.IF_MODIFIED_SINCE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.assertj.core.api.Assertions;
import org.infinispan.Cache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestRawClient;
import org.infinispan.client.rest.RestResponse;
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
            new CacheResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false),
            new CacheResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false),
            new CacheResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true),
            new CacheResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true),
      };
   }

   @Test
   public void testLegacyPredefinedCache() {
      putStringValueInCache("rest", "k1", "v1");

      CompletionStage<RestResponse> response = client.cache("rest").get("k1");

      assertThat(response).isOk();
   }

   @Test
   public void shouldReadWriteToLegacyCache() {
      //given
      putStringValueInCache("legacy", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("legacy").get("test", TEXT_PLAIN_TYPE);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType("text/plain");
      assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldConvertExistingSerializableObjectToJson() {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("objectCache", "test".getBytes(), testClass);

      //when
      CompletionStage<RestResponse> response = client.cache("objectCache").get("test", APPLICATION_JSON_TYPE);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType("application/json");
      assertThat(response).hasReturnedText("{\"" + TYPE + "\":\"" + TestClass.class.getName() + "\",\"name\":\"test\"}");
   }

   @Test
   public void shouldConvertExistingSerializableObjectToXml() {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("objectCache", "test".getBytes(), testClass);

      //when
      CompletionStage<RestResponse> response = client.cache("objectCache").get("test", APPLICATION_XML_TYPE);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType("application/xml");
      assertThat(response).hasReturnedText(
            "<?xml version=\"1.0\" ?><org.infinispan.rest.TestClass><name>test</name></org.infinispan.rest.TestClass>");
   }

   @Test
   public void shouldReadAsBinaryWithPojoCache() {
      //given
      String cacheName = "pojoCache";
      String key = "test";
      TestClass value = new TestClass();
      value.setName("test");

      putValueInCache(cacheName, key, value);

      //when
      RestResponse response = get(cacheName, key, APPLICATION_OCTET_STREAM_TYPE);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldReadTextWithPojoCache() {
      //given
      String cacheName = "pojoCache";
      String key = "k1";
      String value = "v1";

      putValueInCache(cacheName, key, value);

      //when
      RestResponse response = get(cacheName, key, TEXT_PLAIN_TYPE);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(TEXT_PLAIN_TYPE);
      assertThat(response).hasReturnedText(value);
   }

   @Test
   public void shouldReadByteArrayWithPojoCache() {
      //given
      Cache cache = restServer().getCacheManager().getCache("pojoCache").getAdvancedCache()
            .withEncoding(IdentityEncoder.class);
      cache.put("k1", "v1".getBytes());

      //when
      CompletionStage<RestResponse> response = client.cache("pojoCache").get("k1", APPLICATION_OCTET_STREAM_TYPE);

      //then
      assertThat(response).hasReturnedBytes("v1".getBytes());
      assertThat(response).isOk();
      assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldReadAsJsonWithPojoCache() {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("pojoCache", "test", testClass);

      //when
      CompletionStage<RestResponse> response = client.cache("pojoCache").get("test", APPLICATION_JSON_TYPE);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(APPLICATION_JSON_TYPE);
      assertThat(response).hasReturnedText("{\"" + TYPE + "\":\"org.infinispan.rest.TestClass\",\"name\":\"test\"}");
   }

   @Test
   public void shouldNegotiateFromPojoCacheWithoutAccept() {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      String cacheName = "pojoCache";
      String key = "k1";

      putValueInCache(cacheName, key, testClass);

      //when
      RestResponse response = get(cacheName, key, null);

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(MediaType.TEXT_PLAIN_TYPE);
      assertThat(response).hasReturnedText(testClass.toString());
   }

   @Test
   public void shouldWriteTextContentWithPjoCache() {
      //given
      putStringValueInCache("pojoCache", "key1", "data");

      //when
      CompletionStage<RestResponse> response = client.cache("pojoCache").get("key1", TEXT_PLAIN_TYPE);

      //then
      assertThat(response).isOk();
      assertThat(response).hasReturnedText("data");
      assertThat(response).hasContentType(TEXT_PLAIN_TYPE);
   }

   @Test
   public void shouldWriteOctetStreamToDefaultCache() {
      //given
      putBinaryValueInCache("default", "keyA", "<hey>ho</hey>".getBytes(), MediaType.APPLICATION_OCTET_STREAM);
      //when
      CompletionStage<RestResponse> response = client.cache("default").get("keyA");

      //then
      assertThat(response).isOk();
      assertThat(response).hasReturnedBytes("<hey>ho</hey>".getBytes());
      assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldIgnoreDisabledCaches() {
      putStringValueInCache("default", "K", "V");
      RestCacheClient cacheClient = client.cache("default");

      CompletionStage<RestResponse> response = cacheClient.get("K");
      assertThat(response).isOk();

      restServer().ignoreCache("default");
      response = cacheClient.get("K");
      assertThat(response).isServiceUnavailable();

      restServer().unignoreCache("default");
      response = cacheClient.get("K");
      assertThat(response).isOk();
   }

   @Test
   public void shouldDeleteExistingValueEvenWithoutMetadata() {
      putValueInCache("default", "test".getBytes(), "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").remove("test");
      //then
      assertThat(response).isOk();
      Assertions.assertThat(restServer().getCacheManager().getCache("default")).isEmpty();
   }

   @Test
   public void testCORSPreflight() {
      String url = String.format("/rest/v2/caches/%s/%s", "default", "key");
      RestRawClient rawClient = client.raw();

      putValueInCache("default", "key", "value");

      Map<String, String> headers = new HashMap<>();
      headers.put(HOST.toString(), "localhost");
      headers.put(ORIGIN.toString(), "http://localhost:" + restServer().getPort());
      headers.put(ACCESS_CONTROL_REQUEST_METHOD.toString(), "GET");

      CompletionStage<RestResponse> preFlight = rawClient.options(url, headers);

      assertThat(preFlight).isOk();
      assertThat(preFlight).hasNoContent();
      assertThat(preFlight).containsAllHeaders(ACCESS_CONTROL_ALLOW_ORIGIN.toString(), ACCESS_CONTROL_ALLOW_METHODS.toString(), ACCESS_CONTROL_ALLOW_HEADERS.toString());
      assertThat(preFlight).hasHeaderWithValues(ACCESS_CONTROL_ALLOW_HEADERS.toString(), (String[]) RequestHeader.toArray());
   }

   @Test
   public void testCorsGET() {
      int port = restServer().getPort();

      putStringValueInCache("default", "test", "test");

      Map<String, String> headers = singletonMap(ORIGIN.toString(), "http://127.0.0.1:" + port);
      CompletionStage<RestResponse> response = client.cache("default").get("test", headers);

      assertThat(response).isOk();
      assertThat(response).containsAllHeaders("access-control-allow-origin");
      assertThat(response).hasHeaderWithValues(ACCESS_CONTROL_EXPOSE_HEADERS.toString(), (String[]) ResponseHeader.toArray());
   }

   @Test
   public void testCorsAllowedJVMProp() {
      CompletionStage<RestResponse> response = client.raw()
            .get("/rest/v2/caches", singletonMap(ORIGIN.toString(), "http://infinispan.org"));

      assertThat(response).isOk();
      assertThat(response).containsAllHeaders("access-control-allow-origin");
   }

   @Test
   public void testCorsSameOrigin() {
      Map<String, String> headers = new HashMap<>();
      String scheme = ssl ? "https://" : "http://";
      headers.put(ORIGIN.toString(), scheme + "origin-host.org");
      headers.put(HOST.toString(), "origin-host.org");

      CompletionStage<RestResponse> response = client.raw().get("/rest/v2/caches", headers);

      assertThat(response).isOk();
   }

   @Test
   public void testCORSAllOrigins() throws IOException {
      RestServerHelper restServerHelper = null;
      RestClient client = null;
      try {
         RestServerConfigurationBuilder restBuilder = new RestServerConfigurationBuilder();
         restBuilder.cors().addNewRule().allowOrigins(new String[]{"*"});
         restBuilder.host("localhost").port(0);
         restServerHelper = RestServerHelper.defaultRestServer();

         RestServerConfiguration build = restBuilder.build();

         restServerHelper.withConfiguration(build).start("test");
         client = restServerHelper.createClient();

         RestResponse response = join(client.cache("default")
               .get("test", singletonMap(ORIGIN.toString(), "http://host.example.com:5576")));
         assertThat(response).containsAllHeaders("access-control-allow-origin");
      } finally {
         client.close();
         if (restServerHelper != null) restServerHelper.stop();
      }
   }

   @Test
   public void testIfModifiedHeaderForCache() {
      putStringValueInCache("expiration", "test", "test");

      RestCacheClient cacheClient = client.cache("expiration");

      RestResponse resp = join(cacheClient.get("test"));
      String dateLast = resp.headers().get("Last-Modified").get(0);

      CompletionStage<RestResponse> sameLastModAndIfModified = cacheClient.get("test", createHeaders(IF_MODIFIED_SINCE, dateLast));
      assertThat(sameLastModAndIfModified).isNotModified();

      putStringValueInCache("expiration", "test", "test-new");
      RestResponse lastmodAfterIfModified = join(cacheClient.get("test"));
      dateLast = lastmodAfterIfModified.headers().get("Last-Modified").get(0);
      assertThat(lastmodAfterIfModified).isOk();

      Map<String, String> header = createHeaders(IF_MODIFIED_SINCE, plus1Day(dateLast));
      CompletionStage<RestResponse> lastmodBeforeIfModified = cacheClient.get("test", header);
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

      String path = String.format("/rest/v2/caches/%s/%s", "default", "k");
      RestResponse response = join(client.raw().get(path, singletonMap(ACCEPT_ENCODING.toString(), "none")));

      assertThat(response).hasNoContentEncoding();
      assertThat(response).hasContentLength(payload.getBytes().length);

      response = join(client.raw().get(path, singletonMap(ACCEPT_ENCODING.toString(), "gzip")));

      assertThat(response).hasGzipContentEncoding();
      assertEquals(decompress(response.getBodyAsByteArray()), payload);
   }

   @Test
   public void testReplaceExistingObject() throws Exception {
      String initialJson = "{\"" + TYPE + "\":\"org.infinispan.rest.TestClass\",\"name\":\"test\"}";
      String changedJson = "{\"" + TYPE + "\":\"org.infinispan.rest.TestClass\",\"name\":\"test2\"}";

      RestResponse response = writeJsonToCache("key", initialJson, "objectCache");
      assertThat(response).isOk();

      response = writeJsonToCache("key", changedJson, "objectCache");
      assertThat(response).isOk();

      response = join(client.cache("objectCache").get("key", APPLICATION_JSON_TYPE));

      JsonNode jsonNode = new ObjectMapper().readTree(response.getBody());
      assertEquals(jsonNode.get("name").asText(), "test2");
   }

   private RestResponse writeJsonToCache(String key, String json, String cacheName) {
      RestEntity restEntity = RestEntity.create(APPLICATION_JSON, json);
      return join(client.cache(cacheName).put(key, restEntity));
   }

   @Test
   public void testServerDeserialization() throws Exception {
      Object value = new Person();

      byte[] jsonMarshalled = (byte[]) new JsonTranscoder().transcode(value, APPLICATION_OBJECT, APPLICATION_JSON);
      byte[] xmlMarshalled = (byte[]) new XMLTranscoder().transcode(value, APPLICATION_OBJECT, APPLICATION_XML);
      byte[] javaMarshalled = new JavaSerializationMarshaller().objectToByteBuffer(value);

      String expectError = "Class '" + value.getClass().getName() + "' blocked by deserialization white list";

      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, jsonMarshalled);
      RestEntity xmlEntity = RestEntity.create(APPLICATION_XML, xmlMarshalled);
      RestEntity javaEntity = RestEntity.create(APPLICATION_SERIALIZED_OBJECT, javaMarshalled);

      CompletionStage<RestResponse> jsonResponse = client.cache("objectCache").put("addr2", jsonEntity);
      assertThat(jsonResponse).isError();
      assertThat(jsonResponse).containsReturnedText(expectError);

      CompletionStage<RestResponse> xmlResponse = client.cache("objectCache").put("addr3", xmlEntity);
      assertThat(xmlResponse).isError();
      assertThat(xmlResponse).containsReturnedText(expectError);

      CompletionStage<RestResponse> serializationResponse = client.cache("objectCache").put("addr4", javaEntity);
      assertThat(serializationResponse).isError();
      assertThat(serializationResponse).containsReturnedText(expectError);

   }
}
