package org.infinispan.rest.resources;

import static java.util.Collections.singletonMap;
import static org.infinispan.client.rest.RestHeaders.ACCEPT;
import static org.infinispan.client.rest.RestHeaders.ACCEPT_ENCODING;
import static org.infinispan.client.rest.RestHeaders.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.infinispan.client.rest.RestHeaders.ACCESS_CONTROL_ALLOW_METHODS;
import static org.infinispan.client.rest.RestHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.infinispan.client.rest.RestHeaders.ACCESS_CONTROL_EXPOSE_HEADERS;
import static org.infinispan.client.rest.RestHeaders.ACCESS_CONTROL_REQUEST_METHOD;
import static org.infinispan.client.rest.RestHeaders.CONTENT_ENCODING;
import static org.infinispan.client.rest.RestHeaders.HOST;
import static org.infinispan.client.rest.RestHeaders.ORIGIN;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.test.skip.SkipTestNG.skipIf;
import static org.infinispan.rest.RequestHeader.IF_MODIFIED_SINCE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
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
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.dataconversion.Compression;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.DateUtils;
import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.ResponseHeader;
import org.infinispan.rest.TestClass;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;


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
            new CacheResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(false),
            new CacheResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(true),
            new CacheResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(false),
            new CacheResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(true),
            new CacheResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(false),
            new CacheResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(true),
            new CacheResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(false),
            new CacheResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(true),
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
   public void shouldConvertExistingSerializableObjectToXml() {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      RestCacheClient objectCache = client.cache("objectCache");
      String xml = "<org.infinispan.rest.TestClass><name>test</name></org.infinispan.rest.TestClass>";
      join(objectCache.put("test", RestEntity.create(APPLICATION_XML, xml)));

      //when
      RestResponse response = join(objectCache.get("test", APPLICATION_XML_TYPE));

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType("application/xml");
      assertThat(response).hasReturnedText(xml);
   }

   @Test
   public void shouldReadAsBinaryWithPojoCache() {
      //given
      RestCacheClient pojoCache = client.cache("pojoCache");
      String key = "test";
      TestClass value = new TestClass();
      value.setName("test");

      join(pojoCache.put(key, value.toJson().toString()));

      //when
      RestResponse response = join(pojoCache.get(key, APPLICATION_OCTET_STREAM_TYPE));

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldReadTextWithPojoCache() {
      //given
      RestCacheClient pojoCache = client.cache("pojoCache");
      String key = "k1";
      String value = "v1";

      join(pojoCache.put(key, value));

      //when
      RestResponse response = join(pojoCache.get(key));

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(TEXT_PLAIN_TYPE);
      assertThat(response).hasReturnedText(value);
   }

   @Test
   public void shouldReadByteArrayWithPojoCache() {
      //given
      Cache cache = restServer().getCacheManager().getCache("pojoCache").getAdvancedCache();
      cache.put("k1", "v1".getBytes());

      //when
      CompletionStage<RestResponse> response = client.cache("pojoCache").get("k1", APPLICATION_OCTET_STREAM_TYPE);

      //then
      assertThat(response).hasReturnedBytes("v1".getBytes());
      assertThat(response).isOk();
      assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldNegotiateFromPojoCacheWithoutAccept() {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      String json = testClass.toJson().toString();
      RestCacheClient pojoCache = client.cache("pojoCache");
      String key = "k1";

      join(pojoCache.put("k1", json));

      //when
      RestResponse response = join(pojoCache.get(key, Collections.emptyMap()));

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType(MediaType.TEXT_PLAIN_TYPE);
      assertThat(response).hasReturnedText(json);
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
      CompletionStage<RestResponse> response = client.cache("default").get("keyA", Map.of("Accept", APPLICATION_OCTET_STREAM_TYPE));

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

      if (security) {
         Security.doAs(TestingUtil.makeSubject(AuthorizationPermission.ADMIN.name()),() -> restServer().ignoreCache("default"));
      } else {
         restServer().ignoreCache("default");
      }

      response = cacheClient.get("K");
      assertThat(response).isServiceUnavailable();

      if (security) {
         Security.doAs(TestingUtil.makeSubject(AuthorizationPermission.ADMIN.name()), () -> restServer().unignoreCache("default"));
      } else {
         restServer().unignoreCache("default");
      }

      response = cacheClient.get("K");
      assertThat(response).isOk();
   }

   @Test
   public void shouldDeleteExistingValueEvenWithoutMetadata() {
      RestCacheClient defaultCache = client.cache("default");
      join(defaultCache.put("test", "test"));

      //when
      CompletionStage<RestResponse> response = defaultCache.remove("test");
      //then
      assertThat(response).isOk();
      Assertions.assertThat(join(defaultCache.size()).body()).isEqualTo("0");
   }

   @Test
   public void testCORSPreflight() {
      String url = String.format("/rest/v2/caches/%s/%s", "default", "key");
      RestRawClient rawClient = client.raw();

      join(client.cache("default").put("key", "value"));

      Map<String, String> headers = new HashMap<>();
      headers.put(HOST, "localhost");
      headers.put(ORIGIN, "http://localhost:" + restServer().getPort());
      headers.put(ACCESS_CONTROL_REQUEST_METHOD, "GET");

      CompletionStage<RestResponse> preFlight = rawClient.options(url, headers);

      assertThat(preFlight).isOk();
      assertThat(preFlight).hasNoContent();
      assertThat(preFlight).containsAllHeaders(ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_HEADERS);
      assertThat(preFlight).hasHeaderWithValues(ACCESS_CONTROL_ALLOW_HEADERS, (String[]) RequestHeader.toArray());
   }

   @Test
   public void testCorsGET() {
      int port = restServer().getPort();

      putStringValueInCache("default", "test", "test");

      Map<String, String> headers = singletonMap(ORIGIN, "http://127.0.0.1:" + port);
      CompletionStage<RestResponse> response = client.cache("default").get("test", headers);

      assertThat(response).isOk();
      assertThat(response).containsAllHeaders("access-control-allow-origin");
      assertThat(response).hasHeaderWithValues(ACCESS_CONTROL_EXPOSE_HEADERS, (String[]) ResponseHeader.toArray());
   }

   @Test
   public void testCorsAllowedJVMProp() {
      CompletionStage<RestResponse> response = client.raw()
            .get("/rest/v2/caches", singletonMap(ORIGIN, "http://infinispan.org"));

      assertThat(response).isOk();
      assertThat(response).containsAllHeaders("access-control-allow-origin");
   }

   @Test
   public void testCorsSameOrigin() {
      skipIf(protocol == HTTP_20, "Skipping for HTTP/2");
      Map<String, String> headers = new HashMap<>();
      String scheme = ssl ? "https://" : "http://";
      headers.put(ORIGIN, scheme + "origin-host.org");
      headers.put(HOST, "origin-host.org");

      CompletionStage<RestResponse> response = client.raw().get("/rest/v2/caches", headers);

      assertThat(response).isOk();
   }

   @Test
   public void testCORSAllOrigins() throws Exception {
      RestServerHelper restServerHelper = null;
      RestClient client = null;
      try {
         RestServerConfigurationBuilder restBuilder = new RestServerConfigurationBuilder();
         restBuilder.cors().addNewRule().allowOrigins(new String[]{"*"});
         restBuilder.host("localhost").port(0);
         restServerHelper = RestServerHelper.defaultRestServer();
         RestServerConfiguration build = restBuilder.build();
         restServerHelper.serverConfigurationBuilder().read(build);
         configureServer(restServerHelper);
         restServerHelper.start("test");
         RestClientConfigurationBuilder clientConfig = getClientConfig("admin", "admin");
         clientConfig.clearServers().addServer().host("localhost").port(restServerHelper.getPort());
         client = RestClient.forConfiguration(clientConfig.build());
         RestResponse response = join(client.cache("default")
               .get("test", singletonMap(ORIGIN, "http://host.example.com:5576")));
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
      ZonedDateTime plus = DateUtils.parseRFC1123(rfc1123Date).plusDays(1);
      return DateUtils.toRFC1123(plus.toEpochSecond() * 1000);
   }

   @Test
   public void testCompression() {
      String payload = "A long time ago in a galaxy far, far away....";

      // Write
      for (Compression compression : Arrays.asList(Compression.GZIP, Compression.DEFLATE)) {
         String path = String.format("/rest/v2/caches/%s/%s", "default", k(0, compression.name()));
         RestResponse response = join(client.raw().put(path, Map.of(CONTENT_ENCODING, compression.name()), RestEntity.create(MediaType.TEXT_PLAIN, compression.compress(payload))));
         assertThat(response).isOk();
         response = join(client.raw().get(path));
         assertThat(response).isOk();
         assertEquals(payload, response.body());
      }

      // Read
      for (Compression compression : Arrays.asList(Compression.GZIP, Compression.DEFLATE)) {
         String path = String.format("/rest/v2/caches/%s/%s", "default", k(1, compression.name()));
         RestResponse response = join(client.raw().put(path, RestEntity.create(MediaType.TEXT_PLAIN, payload)));
         assertThat(response).isOk();
         response = join(client.raw().get(path, Map.of(ACCEPT, MediaType.TEXT_PLAIN.toString(), ACCEPT_ENCODING, compression.name())));
         assertThat(response).hasContentEncoding(compression.name());
         byte[] body = response.bodyAsByteArray();
         assertEquals(payload, compression.decompress(body));
      }
   }

}
