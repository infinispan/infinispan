package org.infinispan.rest;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.rest.JSONConstants.TYPE;

import org.assertj.core.api.Assertions;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.RestOperationsTest")
public class RestOperationsTest extends BaseRestOperationsTest {

   public ConfigurationBuilder getDefaultCacheBuilder() {
      return new ConfigurationBuilder();
   }

   @Override
   protected boolean enableCompatibility() {
      return true;
   }

   @Override
   protected void defineCaches() {
      super.defineCaches();
      ConfigurationBuilder object = getDefaultCacheBuilder();
      object.encoding().key().mediaType(TEXT_PLAIN_TYPE);
      object.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);

      ConfigurationBuilder legacyStorageCache = getDefaultCacheBuilder();
      legacyStorageCache.encoding().key().mediaType("application/x-java-object;type=java.lang.String");

      restServer.defineCache("objectCache", object);
      restServer.defineCache("legacy", legacyStorageCache);

   }

   @Test
   public void shouldReadWriteToLegacyCache() throws Exception {
      //given
      putStringValueInCache("legacy", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "legacy", "test"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldConvertExistingSerializableObjectToJson() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("objectCache", "test".getBytes(), testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "objectCache", "test"))
            .header(HttpHeader.ACCEPT, "application/json")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      ResponseAssertion.assertThat(response).hasReturnedText("{\"" + TYPE + "\":\"" + TestClass.class.getName() + "\",\"name\":\"test\"}");
   }

   @Test
   public void shouldConvertExistingSerializableObjectToXml() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("objectCache", "test".getBytes(), testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "objectCache", "test"))
            .header(HttpHeader.ACCEPT, "application/xml")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/xml");
      ResponseAssertion.assertThat(response).hasReturnedText(
            "<org.infinispan.rest.TestClass>\n" +
                  "  <name>test</name>\n" +
                  "</org.infinispan.rest.TestClass>");
   }

   @Test
   public void shouldReadAsBinaryWithCompatMode() throws Exception {
      //given
      String cacheName = "compatCache";
      String key = "test";
      TestClass value = new TestClass();
      value.setName("test");

      putValueInCache(cacheName, key, value);

      //when
      ContentResponse response = get(cacheName, key, APPLICATION_OCTET_STREAM_TYPE);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldReadTextWithCompatMode() throws Exception {
      //given
      String cacheName = "compatCache";
      String key = "k1";
      String value = "v1";

      putValueInCache(cacheName, key, value);

      //when
      ContentResponse response = get(cacheName, key, TEXT_PLAIN_TYPE);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(TEXT_PLAIN_TYPE);
      ResponseAssertion.assertThat(response).hasReturnedText(value);
   }

   @Test
   public void shouldReadByteArrayWithCompatMode() throws Exception {
      //given
      Cache cache = restServer.getCacheManager().getCache("compatCache").getAdvancedCache()
            .withEncoding(IdentityEncoder.class);
      cache.put("k1", "v1".getBytes());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "compatCache", "k1"))
            .header(HttpHeader.ACCEPT, APPLICATION_OCTET_STREAM_TYPE)
            .send();

      //then
      ResponseAssertion.assertThat(response).hasReturnedBytes("v1".getBytes());
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldReadAsJsonWithCompatMode() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("compatCache", "test", testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "compatCache", "test"))
            .header(HttpHeader.ACCEPT, MediaType.APPLICATION_JSON_TYPE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(MediaType.APPLICATION_JSON_TYPE);
      ResponseAssertion.assertThat(response).hasReturnedText("{\"" + TYPE + "\":\"org.infinispan.rest.TestClass\",\"name\":\"test\"}");
   }

   @Test
   public void shouldNegotiateFromCompatCacheWithoutAccept() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      String cacheName = "compatCache";
      String key = "k1";

      putValueInCache(cacheName, key, testClass);

      //when
      ContentResponse response = get(cacheName, key, null);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(MediaType.TEXT_PLAIN_TYPE);
      ResponseAssertion.assertThat(response).hasReturnedText(testClass.toString());
   }

   @Test
   public void shouldWriteTextContentWithCompatMode() throws Exception {
      //given
      putStringValueInCache("compatCache", "key1", "data");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "compatCache", "key1"))
            .header(HttpHeader.ACCEPT, TEXT_PLAIN_TYPE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasReturnedText("data");
      ResponseAssertion.assertThat(response).hasContentType(TEXT_PLAIN_TYPE);
   }

   @Test
   public void shouldWriteOctetStreamToDefaultCache() throws Exception {
      //given
      putBinaryValueInCache("default", "keyA", "<hey>ho</hey>".getBytes(), MediaType.APPLICATION_OCTET_STREAM);
      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "keyA"))
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasReturnedBytes("<hey>ho</hey>".getBytes());
      ResponseAssertion.assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldIgnoreDisabledCaches() throws Exception {
      putStringValueInCache("default", "K", "V");
      String url = String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "K");

      ContentResponse response = client.newRequest(url).send();
      ResponseAssertion.assertThat(response).isOk();

      restServer.ignoreCache("default");
      response = client.newRequest(url).send();
      ResponseAssertion.assertThat(response).isServiceUnavailable();

      restServer.unignoreCache("default");
      response = client.newRequest(url).send();
      ResponseAssertion.assertThat(response).isOk();
   }

   @Test
   public void shouldDeleteExistingValueEvenWithoutMetadata() throws Exception {
      putValueInCache("default", "test".getBytes(), "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.DELETE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(restServer.getCacheManager().getCache("default")).isEmpty();
   }
}
