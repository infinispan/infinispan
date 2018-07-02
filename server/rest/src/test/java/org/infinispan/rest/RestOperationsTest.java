package org.infinispan.rest;

import static org.eclipse.jetty.http.HttpHeader.ACCEPT_ENCODING;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING_TYPE;
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
import static org.testng.Assert.assertEquals;

import org.assertj.core.api.Assertions;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.rest.search.entity.Person;
import org.infinispan.server.core.dataconversion.JsonTranscoder;
import org.infinispan.server.core.dataconversion.XMLTranscoder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.RestOperationsTest")
public class RestOperationsTest extends BaseRestOperationsTest {

   public ConfigurationBuilder getDefaultCacheBuilder() {
      return new ConfigurationBuilder();
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
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "objectCache", "test"))
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
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "objectCache", "test"))
            .header(HttpHeader.ACCEPT, "application/xml")
            .send();

      //then
      assertThat(response).isOk();
      assertThat(response).hasContentType("application/xml");
      assertThat(response).hasReturnedText(
            "<org.infinispan.rest.TestClass>\n" +
                  "  <name>test</name>\n" +
                  "</org.infinispan.rest.TestClass>");
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
      Cache cache = restServer.getCacheManager().getCache("pojoCache").getAdvancedCache()
            .withEncoding(IdentityEncoder.class);
      cache.put("k1", "v1".getBytes());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "pojoCache", "k1"))
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
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "pojoCache", "test"))
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
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "pojoCache", "key1"))
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
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "keyA"))
            .send();

      //then
      assertThat(response).isOk();
      assertThat(response).hasReturnedBytes("<hey>ho</hey>".getBytes());
      assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldIgnoreDisabledCaches() throws Exception {
      putStringValueInCache("default", "K", "V");
      String url = String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "K");

      ContentResponse response = client.newRequest(url).send();
      assertThat(response).isOk();

      restServer.ignoreCache("default");
      response = client.newRequest(url).send();
      assertThat(response).isServiceUnavailable();

      restServer.unignoreCache("default");
      response = client.newRequest(url).send();
      assertThat(response).isOk();
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
      assertThat(response).isOk();
      Assertions.assertThat(restServer.getCacheManager().getCache("default")).isEmpty();
   }

   @Test
   public void testCORSPreflight() throws Exception {
      putValueInCache("default", "key", "value");

      ContentResponse preFlight = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "key"))
            .method(HttpMethod.OPTIONS)
            .header(HttpHeader.HOST, "localhost")
            .header(HttpHeader.ORIGIN, "http://localhost:80")
            .header("access-control-request-method", "GET")
            .send();


      assertThat(preFlight).isOk();
      assertThat(preFlight).hasNoContent();
      assertThat(preFlight).containsAllHeaders("access-control-allow-origin", "access-control-allow-methods");
   }

   @Test
   public void testCorsGET() throws Exception {
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ORIGIN, "http://127.0.0.1:80")
            .send();

      assertThat(response).isOk();
      assertThat(response).containsAllHeaders("access-control-allow-origin");
   }

   @Test
   public void testCompression() throws Exception {
      String payload = getResourceAsString("person.proto", getClass().getClassLoader());
      putStringValueInCache("default", "k", payload);

      HttpClient uncompressingClient = new HttpClient();
      uncompressingClient.start();
      uncompressingClient.getContentDecoderFactories().clear();

      ContentResponse response = uncompressingClient
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "k"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      assertThat(response).hasNoContentEncoding();
      assertThat(response).hasContentLength(payload.getBytes().length);
      client.getContentDecoderFactories().clear();

      response = uncompressingClient
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "k"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .header(ACCEPT_ENCODING, "gzip")
            .send();

      assertThat(response).hasGzipContentEncoding();
      assertEquals(decompress(response.getContent()), payload);
   }

   @Test
   public void testServerDeserialization() throws Exception {
      Object value = new Person();

      byte[] jbossMarshalled = new GenericJBossMarshaller().objectToByteBuffer(value);
      byte[] jsonMarshalled = (byte[]) new JsonTranscoder().transcode(value, APPLICATION_OBJECT, APPLICATION_JSON);
      byte[] xmlMarshalled = (byte[]) new XMLTranscoder().transcode(value, APPLICATION_OBJECT, APPLICATION_XML);
      byte[] javaMarshalled = new JavaSerializationMarshaller().objectToByteBuffer(value);

      String expectError = "Class '" + value.getClass().getName() + "' blocked by deserialization white list";

      ContentResponse response1 = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "objectCache", "addr1"))
            .content(new BytesContentProvider(jbossMarshalled))
            .header(HttpHeader.CONTENT_TYPE, APPLICATION_JBOSS_MARSHALLING_TYPE)
            .method(HttpMethod.PUT)
            .send();

      assertThat(response1).isError();
      assertThat(response1).containsReturnedText(expectError);

      ContentResponse response2 = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "objectCache", "addr2"))
            .content(new BytesContentProvider(jsonMarshalled))
            .header(HttpHeader.CONTENT_TYPE, APPLICATION_JSON_TYPE)
            .method(HttpMethod.PUT)
            .send();

      assertThat(response2).isError();
      assertThat(response2).containsReturnedText(expectError);

      ContentResponse response3 = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "objectCache", "addr3"))
            .content(new BytesContentProvider(xmlMarshalled))
            .header(HttpHeader.CONTENT_TYPE, APPLICATION_XML_TYPE)
            .method(HttpMethod.PUT)
            .send();

      assertThat(response3).isError();
      assertThat(response3).containsReturnedText(expectError);

      ContentResponse response4 = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "objectCache", "addr4"))
            .content(new BytesContentProvider(javaMarshalled))
            .header(HttpHeader.CONTENT_TYPE, APPLICATION_SERIALIZED_OBJECT_TYPE)
            .method(HttpMethod.PUT)
            .send();

      assertThat(response4).isError();
      assertThat(response4).containsReturnedText(expectError);

   }

}
