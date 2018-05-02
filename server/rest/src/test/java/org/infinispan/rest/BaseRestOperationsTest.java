package org.infinispan.rest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.dataconversion.StandardConversions.bytesToHex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.assertj.core.api.Assertions;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional")
public abstract class BaseRestOperationsTest extends AbstractInfinispanTest {
   protected HttpClient client;
   protected RestServerHelper restServer;

   protected abstract ConfigurationBuilder getDefaultCacheBuilder();

   protected abstract boolean enableCompatibility();

   @BeforeClass
   public void beforeSuite() throws Exception {
      restServer = RestServerHelper.defaultRestServer(getDefaultCacheBuilder(), "default");
      defineCaches();
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      client = new HttpClient();
      client.start();
   }

   private static final long DEFAULT_LIFESPAN = 45190;
   private static final long DEFAULT_MAX_IDLE = 1859446;

   protected void defineCaches() {
      ConfigurationBuilder expirationConfiguration = getDefaultCacheBuilder();
      expirationConfiguration.expiration().lifespan(DEFAULT_LIFESPAN).maxIdle(DEFAULT_MAX_IDLE);

      ConfigurationBuilder xmlCacheConfiguration = getDefaultCacheBuilder();
      xmlCacheConfiguration.encoding().value().mediaType("application/xml");

      ConfigurationBuilder jsonCacheConfiguration = getDefaultCacheBuilder();
      jsonCacheConfiguration.encoding().value().mediaType("application/json");

      ConfigurationBuilder octetStreamCacheConfiguration = getDefaultCacheBuilder();
      octetStreamCacheConfiguration.encoding().value().mediaType("application/octet-stream");

      ConfigurationBuilder unknownContentCacheConfiguration = getDefaultCacheBuilder();
      unknownContentCacheConfiguration.encoding().value().mediaType("application/unknown");

      ConfigurationBuilder javaSerialized = getDefaultCacheBuilder();
      javaSerialized.encoding().value().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE);

      ConfigurationBuilder text = getDefaultCacheBuilder();
      text.encoding().key().mediaType(TEXT_PLAIN_TYPE);
      text.encoding().value().mediaType(TEXT_PLAIN_TYPE);

      ConfigurationBuilder compat = getDefaultCacheBuilder();
      compat.compatibility().enabled(enableCompatibility());

      restServer.defineCache("expiration", expirationConfiguration);
      restServer.defineCache("xml", xmlCacheConfiguration);
      restServer.defineCache("json", jsonCacheConfiguration);
      restServer.defineCache("binary", octetStreamCacheConfiguration);
      restServer.defineCache("unknown", unknownContentCacheConfiguration);
      restServer.defineCache("serialized", javaSerialized);
      restServer.defineCache("textCache", text);
      restServer.defineCache("compatCache", compat);

   }


   @AfterClass
   public void afterSuite() throws Exception {
      client.stop();
      restServer.stop();
   }

   @AfterMethod
   public void afterMethod() {
      restServer.clear();
   }

   @SuppressWarnings("unchecked")
   public InternalCacheEntry<String, byte[]> getCacheEntry(String cacheName, byte[] key) {
      CacheEntry cacheEntry = getCache(cacheName).getCacheEntry(key);
      return (InternalCacheEntry<String, byte[]>) cacheEntry;
   }

   public AdvancedCache getCache(String cacheName) {
      return restServer.getCacheManager().getCache(cacheName, false).getAdvancedCache().withKeyEncoding(IdentityEncoder.class);
   }

   @Test
   public void shouldGetNonExistingValue() throws Exception {
      //when
      ContentResponse response = client.GET("http://localhost:" + restServer.getPort() + "/rest/default/nonExisting");

      //then
      ResponseAssertion.assertThat(response).doesntExist();
   }

   @Test
   public void shouldReturnNotExistingOnWrongContext() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/wrongContext/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      //then
      ResponseAssertion.assertThat(response).doesntExist();
   }

   @Test
   public void shouldGetAsciiValueStoredInSpecificFormat() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldHaveProperEtagWhenGettingValue() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      //then
      ResponseAssertion.assertThat(response).hasEtag();
      ResponseAssertion.assertThat(response).hasHeaderMatching("ETag", "-\\d+");
   }

   @Test
   public void shouldReturnExtendedHeaders() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s?extended=true", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      //then
      ResponseAssertion.assertThat(response).hasExtendedHeaders();
   }

   @Test
   public void shouldGetUtf8ValueStoredInSpecificFormat() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "text/plain;charset=UTF-8")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain;charset=UTF-8");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldGetJsonValueStoredInSpecificFormat() throws Exception {
      //given
      putJsonValueInCache("json", "test", "{\"name\": \"test\"}");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "json", "test"))
            .header(HttpHeader.ACCEPT, "application/json")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      ResponseAssertion.assertThat(response).hasReturnedText("{\"name\": \"test\"}");
   }

   @Test
   public void shouldGetXmlValueStoredInSpecificFormat() throws Exception {
      //given
      putStringValueInCache("xml", "test", "<xml><name>test</name></xml>");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "xml", "test"))
            .header(HttpHeader.ACCEPT, "application/xml")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/xml");
      ResponseAssertion.assertThat(response).hasReturnedText("<xml><name>test</name></xml>");
   }

   @Test
   public void shouldGetValueStoredInUnknownFormat() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldGetOctetStreamValueStoredInSpecificFormat() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putBinaryValueInCache("serialized", "test", convertToBytes(testClass), APPLICATION_SERIALIZED_OBJECT);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "serialized", "test"))
            .send();

      TestClass convertedObject = convertFromBytes(response.getContent(), TestClass.class);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(APPLICATION_SERIALIZED_OBJECT.toString());
      ResponseAssertion.assertThat(response).hasNoCharset();
      Assertions.assertThat(convertedObject.getName()).isEqualTo("test");
   }

   @Test
   public void shouldConvertExistingObjectToText() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldConvertExistingObjectToTextUtf8() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "text/plain;charset=utf-8")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   protected void putValueInCache(String cacheName, Object key, Object testValue) {
      restServer.getCacheManager().getCache(cacheName).getAdvancedCache().put(key, testValue);
   }

   @Test
   public void shouldGetExistingValueWithoutOutputUsingHEAD() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.HEAD)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasNoContent();
   }

   protected void putInCache(String cacheName, Object key, String keyContentType, String value, String contentType) throws InterruptedException, ExecutionException, TimeoutException {
      Request request = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), cacheName, key))
            .content(new StringContentProvider(value))
            .header("Content-type", contentType)
            .method(HttpMethod.PUT);
      if (keyContentType != null) request.header("Key-Content-type", keyContentType);

      ContentResponse response = request.send();
      ResponseAssertion.assertThat(response).isOk();
   }

   protected void putInCache(String cacheName, Object key, String value, String contentType) throws InterruptedException, ExecutionException, TimeoutException {
      putInCache(cacheName, key, null, value, contentType);
   }

   protected void putStringValueInCache(String cacheName, String key, String value) throws InterruptedException, ExecutionException, TimeoutException {
      putInCache(cacheName, key, value, "text/plain; charset=utf-8");
   }

   protected void putJsonValueInCache(String cacheName, String key, String value) throws InterruptedException, ExecutionException, TimeoutException {
      putInCache(cacheName, key, value, "application/json; charset=utf-8");
   }

   protected void putBinaryValueInCache(String cacheName, String key, byte[] value, MediaType mediaType) throws InterruptedException, ExecutionException, TimeoutException {
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), cacheName, key))
            .content(new BytesContentProvider(value))
            .header(HttpHeader.CONTENT_TYPE, mediaType.toString())
            .method(HttpMethod.PUT)
            .send();
      ResponseAssertion.assertThat(response).isOk();
   }

   private byte[] convertToBytes(Object object) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
           ObjectOutput out = new ObjectOutputStream(bos)) {
         out.writeObject(object);
         return bos.toByteArray();
      }
   }

   private <T> T convertFromBytes(byte[] bytes, Class<T> klass) throws IOException, ClassNotFoundException {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
         ObjectInputStream in = new ObjectInputStream(bis);
         return (T) in.readObject();
      }
   }

   @Test
   public void shouldDeleteExistingValue() throws Exception {
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.DELETE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(restServer.getCacheManager().getCache("default")).isEmpty();
   }

   @Test
   public void shouldDeleteExistingValueWithAcceptHeader() throws Exception {
      putBinaryValueInCache("serialized", "test", convertToBytes(42), APPLICATION_SERIALIZED_OBJECT);

      ContentResponse headResponse = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "serialized", "test"))
            .method(HttpMethod.HEAD)
            .header(HttpHeader.ACCEPT, "application/x-java-serialized-object")
            .send();

      ResponseAssertion.assertThat(headResponse).isOk();
      ResponseAssertion.assertThat(headResponse).hasContentType("application/x-java-serialized-object");


      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "serialized", "test"))
            .method(HttpMethod.DELETE)
            .header(HttpHeader.CONTENT_TYPE, "text/plain;charset=UTF-8")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(restServer.getCacheManager().getCache("binary")).isEmpty();
   }

   @Test
   public void shouldDeleteNonExistingValue() throws Exception {
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "doesnt_exist"))
            .method(HttpMethod.DELETE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void shouldDeleteEntireCache() throws Exception {
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "default"))
            .method(HttpMethod.DELETE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(restServer.getCacheManager().getCache("default")).isEmpty();
   }

   @Test
   public void shouldGetAllEntriesFromEmptyCache() throws Exception {
      //when
      ContentResponse response = client
            .newRequest("http://localhost:" + restServer.getPort() + "/rest/default")
            .method(HttpMethod.GET)
            .header("Content-Type", "text/plain; charset=utf-8")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasReturnedText("");
   }

   @Test
   public void shouldGetAllEntriesConvertedToText() throws Exception {
      //given
      putStringValueInCache("textCache", "key1", "test1");
      putStringValueInCache("textCache", "key2", "test2");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "textCache"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("key1\nkey2");
   }

   @Test
   public void shouldGetAllEntriesConvertedToTextUtf8() throws Exception {
      //given
      putStringValueInCache("textCache", "key1", "test1");
      putStringValueInCache("textCache", "key2", "test2");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "textCache"))
            .header(HttpHeader.ACCEPT, "text/plain;charset=UTF-8")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain;charset=UTF-8");
      ResponseAssertion.assertThat(response).hasReturnedText("key1\nkey2");
   }

   @Test
   public void shouldGetAllEntriesConvertedToTextIso_8859_1() throws Exception {
      //given
      putStringValueInCache("textCache", "key1", "test1");
      putStringValueInCache("textCache", "key2", "test2");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "textCache"))
            .header(HttpHeader.ACCEPT, "text/plain; charset=ISO-8859-1")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain; charset=ISO-8859-1");
      ResponseAssertion.assertThat(response).hasReturnedText("key1\nkey2");
   }

   @Test
   public void shouldGetAllEntriesConvertedToJson() throws Exception {
      //given
      putStringValueInCache("textCache", "key1", "test1");
      putStringValueInCache("textCache", "key2", "test2");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "textCache"))
            .header(HttpHeader.ACCEPT, "application/json")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      ResponseAssertion.assertThat(response).hasReturnedText("keys=[key1,key2]");
   }

   @Test
   public void shouldGetAllEntriesConvertedToXml() throws Exception {
      //given
      putStringValueInCache("textCache", "key1", "test1");
      putStringValueInCache("textCache", "key2", "test2");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "textCache"))
            .header(HttpHeader.ACCEPT, "application/xml")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/xml");
      ResponseAssertion.assertThat(response).hasReturnedText("<?xml version=\"1.0\" encoding=\"UTF-8\"?><keys><key>key1</key><key>key2</key></keys>");
   }

   @Test
   public void shouldGetAllEntriesConvertedToBytes() throws Exception {
      //given
      putStringValueInCache("default", "key1", "test1");
      putStringValueInCache("default", "key2", "test2");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "default"))
            .header(HttpHeader.ACCEPT, MediaType.APPLICATION_OCTET_STREAM_TYPE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
      ResponseAssertion.assertThat(response).hasReturnedText(String.format("%s\n%s", bytesToHex("key1".getBytes()), bytesToHex("key2".getBytes())));
   }

   @Test
   public void shouldAcceptMultipleAcceptHeaderValues() throws Exception {
      //given
      putStringValueInCache("textCache", "key1", "test1");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "textCache", "key1"))
            .header(HttpHeader.ACCEPT, "ignored/wrong , text/plain")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("test1");
   }

   @Test
   public void shouldNotAcceptUnknownContentType() throws Exception {
      //given
      putStringValueInCache("default", "key1", "test1");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "key1"))
            .header(HttpHeader.ACCEPT, "application/wrong-content-type")
            .send();

      //then
      ResponseAssertion.assertThat(response).isNotAcceptable();
   }

   @Test
   public void shouldNotAcceptUnknownContentTypeWithHead() throws Exception {
      //given
      putStringValueInCache("default", "key1", "test1");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "key1"))
            .method(HttpMethod.HEAD)
            .header(HttpHeader.ACCEPT, "garbage")
            .send();
      //then
      ResponseAssertion.assertThat(response).isNotAcceptable();
   }

   @Test
   public void shouldNotReturnValueIfSendingCorrectETag() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse firstResponse = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.GET)
            .send();

      String etagFromFirstCall = firstResponse.getHeaders().get("ETag");

      ContentResponse secondResponse = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header("If-None-Match", etagFromFirstCall)
            .method(HttpMethod.GET)
            .send();

      //then
      Assertions.assertThat(etagFromFirstCall).isNotNull().isNotEmpty();
      ResponseAssertion.assertThat(secondResponse).isNotModified();
   }

   @Test
   public void shouldReturnEntityWhenSendingWrongETag() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header("If-None-Match", "Invalid-etag")
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldPutTextValueInCache() throws Exception {
      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .content(new StringContentProvider("Hey!"))
            .header("Content-type", "text/plain;charset=UTF-8")
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("default", "test".getBytes());

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasEtag();
      Assertions.assertThat(new String(cacheEntry.getValue())).isEqualTo("Hey!");
   }

   @Test
   public void shouldReturnJsonWithDefaultConfig() throws Exception {
      putStringValueInCache("textCache", "test", "Hey!");

      ContentResponse getResponse = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "textCache", "test"))
            .method(HttpMethod.GET)
            .header("Accept", "application/json")
            .send();

      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).hasReturnedText("\"Hey!\"");

   }

   @Test
   public void shouldPutUnknownFormatValueInCache() throws Exception {
      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "unknown", "test"))
            .content(new StringContentProvider("Hey!"))
            .header("Content-type", "application/unknown")
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("unknown", "test".getBytes());

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasEtag();
      Assertions.assertThat(new String(cacheEntry.getValue())).isEqualTo("Hey!");
   }

   @Test
   public void shouldPutSerializedValueInCache() throws Exception {
      //when
      TestClass testClass = new TestClass();
      testClass.setName("test");

      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "serialized", "test"))
            .content(new BytesContentProvider(convertToBytes(testClass)))
            .header("Content-type", "application/x-java-serialized-object")
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("serialized", "test".getBytes());

      TestClass valueFromCache = convertFromBytes(cacheEntry.getValue(), TestClass.class);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasEtag();
      Assertions.assertThat(valueFromCache.getName()).isEqualTo("test");
   }

   @Test
   public void shouldConflictWhenTryingToReplaceExistingEntryUsingPost() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .content(new StringContentProvider("Hey!"))
            .header("Content-type", "text/plain;charset=UTF-8")
            .send();

      //then
      ResponseAssertion.assertThat(response).isConflicted();
   }

   @Test
   public void shouldUpdateEntryWhenReplacingUsingPut() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .content(new StringContentProvider("Hey!"))
            .header("Content-type", "text/plain;charset=UTF-8")
            .method(HttpMethod.PUT)
            .send();

      String valueFromCache = new String(getCacheEntry("default", "test".getBytes()).getValue());

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(valueFromCache).isEqualTo("Hey!");
   }

   @Test
   public void shouldServeHtmlFile() throws Exception {
      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest", restServer.getPort()))
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/html");
      ResponseAssertion.assertThat(response).hasContentEqualToFile("index.html");
   }

   @Test
   public void shouldServeBannerFile() throws Exception {
      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/banner.png", restServer.getPort()))
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("image/png");
      ResponseAssertion.assertThat(response).hasContentEqualToFile("banner.png");
   }

   @Test
   public void shouldPutEntryWithDefaultTllAndIdleTime() throws Exception {
      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "expiration", "test"))
            .content(new StringContentProvider("test"))
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("expiration", "test".getBytes());

      Metadata metadata = cacheEntry.getMetadata();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(metadata.lifespan()).isEqualTo(DEFAULT_LIFESPAN);
      Assertions.assertThat(metadata.maxIdle()).isEqualTo(DEFAULT_MAX_IDLE);
   }

   @Test
   public void shouldPutImmortalEntryWithMinusOneTtlAndIdleTime() throws Exception {
      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "expiration", "test"))
            .content(new StringContentProvider("test"))
            .header("timeToLiveSeconds", "-1")
            .header("maxIdleTimeSeconds", "-1")
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("expiration", "test".getBytes());
      Metadata metadata = cacheEntry.getMetadata();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(metadata.lifespan()).isEqualTo(-1);
      Assertions.assertThat(metadata.maxIdle()).isEqualTo(-1);
   }

   @Test
   public void shouldPutImmortalEntryWithZeroTtlAndIdleTime() throws Exception {
      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "expiration", "test"))
            .content(new StringContentProvider("test"))
            .header("timeToLiveSeconds", "0")
            .header("maxIdleTimeSeconds", "0")
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("expiration", "test".getBytes());
      Metadata metadata = cacheEntry.getMetadata();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(metadata.lifespan()).isEqualTo(DEFAULT_LIFESPAN);
      Assertions.assertThat(metadata.maxIdle()).isEqualTo(DEFAULT_MAX_IDLE);
   }

   @Test
   public void testErrorPropagation() throws Exception {
      putStringValueInCache("xml", "key", "<value/>");

      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "xml", "key"))
            .header(HttpHeader.ACCEPT, "application/json")
            .method(HttpMethod.GET)
            .send();

      ResponseAssertion.assertThat(response).isNotAcceptable();
      ResponseAssertion.assertThat(response).containsReturnedText("Cannot convert to application/json");
   }

   @Test
   public void shouldPutEntryWithTtlAndIdleTime() throws Exception {
      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "expiration", "test"))
            .content(new StringContentProvider("test"))
            .header("timeToLiveSeconds", "50")
            .header("maxIdleTimeSeconds", "50")
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("expiration", "test".getBytes());
      Metadata metadata = cacheEntry.getMetadata();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(metadata.lifespan()).isEqualTo(50 * 1000);
      Assertions.assertThat(metadata.maxIdle()).isEqualTo(50 * 1000);
   }

   @Test
   public void shouldPutLargeObject() throws Exception {
      //when
      ByteBuffer payload = ByteBuffer.allocate(1_000_000);
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "binary", "test"))
            .content(new ByteBufferContentProvider(payload))
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("binary", "test".getBytes());

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasEtag();

      Assertions.assertThat(cacheEntry.getValue().length).isEqualTo(1_000_000);
   }

   @Test
   public void shouldFailTooLargeObject() throws Exception {
      //when
      ByteBuffer payload = ByteBuffer.allocate(1_100_000);
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .content(new ByteBufferContentProvider(payload))
            .send();

      //then
      ResponseAssertion.assertThat(response).isPayloadTooLarge();
   }

   @Test
   public void testWildcardAccept() throws Exception {
      putStringValueInCache("default", "test", "test");

      ContentResponse getResponse = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .accept("*/*")
            .method(HttpMethod.GET)
            .send();

      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).hasReturnedText("test");

   }

   protected ContentResponse get(String cacheName, Object key, String keyContentType, String acceptHeader) throws Exception {
      Request request = client.newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), cacheName, key))
            .method(HttpMethod.GET);
      if (acceptHeader != null) {
         request.accept(acceptHeader);
      }
      if (keyContentType != null) {
         request.header("Key-Content-Type", keyContentType);
      }
      ContentResponse response = request.send();
      ResponseAssertion.assertThat(response).isOk();
      return response;
   }

   protected ContentResponse get(String cacheName, Object key, String acceptHeader) throws Exception {
      Request request = client.newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), cacheName, key))
            .method(HttpMethod.GET);
      if (acceptHeader != null) {
         request.accept(acceptHeader);
      }
      ContentResponse response = request.send();
      ResponseAssertion.assertThat(response).isOk();
      return response;
   }

   @Test
   public void shouldAcceptUrlEncodedContentForDefaultCache() throws Exception {
      String value = "word1 word2";
      String urlEncoded = URLEncoder.encode(value, "UTF-8");
      putBinaryValueInCache("default", "test", urlEncoded.getBytes(UTF_8), APPLICATION_WWW_FORM_URLENCODED);

      ContentResponse getResponse = get("default", "test", TEXT_PLAIN_TYPE);

      ResponseAssertion.assertThat(getResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(getResponse).hasContentType(TEXT_PLAIN_TYPE);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithoutAccept() throws Exception {
      putStringValueInCache("default", "test", "test");

      ContentResponse getResponse = get("default", "test", null);

      ResponseAssertion.assertThat(getResponse).hasReturnedText("test");
      ResponseAssertion.assertThat(getResponse).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithAccept() throws Exception {
      putStringValueInCache("default", "test", "test");

      ContentResponse jsonResponse = get("default", "test", "application/json");

      ResponseAssertion.assertThat(jsonResponse).hasReturnedText("\"test\"");
      ResponseAssertion.assertThat(jsonResponse).hasContentType("application/json");

      ContentResponse xmlResponse = get("default", "test", "text/plain");

      ResponseAssertion.assertThat(xmlResponse).hasReturnedText("test");
      ResponseAssertion.assertThat(xmlResponse).hasContentType("text/plain");

      ContentResponse binaryResponse = get("default", "test", APPLICATION_OCTET_STREAM_TYPE);

      ResponseAssertion.assertThat(binaryResponse).hasReturnedBytes("test".getBytes(UTF_8));
      ResponseAssertion.assertThat(binaryResponse).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithBinary() throws Exception {
      TestClass testClass = new TestClass();
      byte[] javaSerialized = new JavaSerializationMarshaller().objectToByteBuffer(testClass);

      putBinaryValueInCache("default", "test", javaSerialized, APPLICATION_OCTET_STREAM);

      ContentResponse response = get("default", "test", APPLICATION_OCTET_STREAM_TYPE);

      ResponseAssertion.assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
      ResponseAssertion.assertThat(response).hasReturnedBytes(javaSerialized);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithWildcardAccept() throws Exception {
      putStringValueInCache("default", "test", "test");

      ContentResponse getResponse = get("default", "test", "*/*");

      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).hasReturnedText("test");
      ResponseAssertion.assertThat(getResponse).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithMultipleAccept() throws Exception {
      putStringValueInCache("default", "test", "test");

      ContentResponse sameWeightResponse = get("default", "test", "text/html,application/xhtml+xml,*/*");

      ResponseAssertion.assertThat(sameWeightResponse).isOk();
      ResponseAssertion.assertThat(sameWeightResponse).hasReturnedText("test");
      ResponseAssertion.assertThat(sameWeightResponse).hasContentType(APPLICATION_OCTET_STREAM_TYPE);

      ContentResponse weightedResponse = get("default", "test", "text/plain;q=0.1, application/json;q=0.8, */*;q=0.7");

      ResponseAssertion.assertThat(weightedResponse).isOk();
      ResponseAssertion.assertThat(weightedResponse).hasReturnedText("\"test\"");
      ResponseAssertion.assertThat(weightedResponse).hasContentType(APPLICATION_JSON_TYPE);
   }

   @Test
   public void shouldNegotiateFromJsonCacheWithoutAccept() throws Exception {
      String cacheName = "json";
      String key = "1";
      String value = "{\"id\": 1}";

      putJsonValueInCache(cacheName, key, value);

      ContentResponse getResponse = get(cacheName, key, null);

      ResponseAssertion.assertThat(getResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(getResponse).hasContentType(APPLICATION_JSON_TYPE);
   }

   @Test
   public void shouldNegotiateFromJsonCacheWithAccept() throws Exception {
      String cacheName = "json";
      String key = "1";
      String value = "{\"id\": 1}";

      putJsonValueInCache(cacheName, key, value);

      ContentResponse jsonResponse = get(cacheName, key, APPLICATION_JSON_TYPE);

      ResponseAssertion.assertThat(jsonResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(jsonResponse).hasContentType(APPLICATION_JSON_TYPE);

      ContentResponse textResponse = get(cacheName, key, TEXT_PLAIN_TYPE);

      ResponseAssertion.assertThat(textResponse).hasReturnedBytes(value.getBytes(UTF_8));
      ResponseAssertion.assertThat(textResponse).hasContentType(TEXT_PLAIN_TYPE);
   }

   @Test
   public void shouldNegotiateFromJsonCacheWithWildcardAccept() throws Exception {
      String cacheName = "json";
      String key = "1";
      String value = "{\"id\": 1}";

      putJsonValueInCache(cacheName, key, value);

      ContentResponse jsonResponse = get(cacheName, key, "*/*");

      ResponseAssertion.assertThat(jsonResponse).isOk();
      ResponseAssertion.assertThat(jsonResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(jsonResponse).hasContentType(APPLICATION_JSON_TYPE);
   }

   @Test
   public void shouldNegotiateFromJsonCacheWithMultipleAccept() throws Exception {
      String cacheName = "json";
      String key = "1";
      String value = "{\"id\": 1}";

      putJsonValueInCache(cacheName, key, value);

      ContentResponse jsonResponse = get(cacheName, key, "text/html,*/*");

      ResponseAssertion.assertThat(jsonResponse).isOk();
      ResponseAssertion.assertThat(jsonResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(jsonResponse).hasContentType(APPLICATION_JSON_TYPE);

      ContentResponse binaryResponse = get(cacheName, key, "application/xml, text/plain; q=0.71, */*;q=0.7");

      ResponseAssertion.assertThat(binaryResponse).isOk();
      ResponseAssertion.assertThat(binaryResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(binaryResponse).hasContentType(TEXT_PLAIN_TYPE);
   }

   @Test
   public void shouldNegotiateOnlySupportedFromDefaultCacheWithMultipleAccept() throws Exception {
      String value = "<test/>";
      putStringValueInCache("default", "test", value);

      ContentResponse getResponse = get("default", "test", "text/html, application/xml");

      ResponseAssertion.assertThat(getResponse).hasReturnedText("<test/>");
      ResponseAssertion.assertThat(getResponse).hasContentType("application/xml");
   }

   @Test
   public void shouldHandleInvalidPath() throws Exception {
      Request browserRequest = client.newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "asdjsad"))
            .header(HttpHeader.ACCEPT, "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
            .method(HttpMethod.GET);

      ContentResponse response = browserRequest.send();
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void shouldHandleIncompletePath() throws Exception {
      Request req = client.newRequest(String.format("http://localhost:%d/rest/%s?action", restServer.getPort(), "default"))
            .method(HttpMethod.GET);

      ContentResponse response = req.send();
      ResponseAssertion.assertThat(response).isBadRequest();
   }

   @Test
   public void testIntegerKeysXmlToTextValues() throws Exception {
      Integer key = 123;
      String keyContentType = "application/x-java-object;type=java.lang.Integer";
      String valueContentType = "application/xml; charset=UTF-8";
      String value = "<root>test</root>";

      putInCache("default", key, keyContentType, value, valueContentType);
      ContentResponse response = get("default", key, keyContentType, "text/plain");

      ResponseAssertion.assertThat(response).hasReturnedText(value);
   }

   @Test
   public void testIntKeysAndJSONToTextValues() throws Exception {
      Integer key = 1234;
      String keyContentType = "application/x-java-object;type=java.lang.Integer";
      String value = "{\"a\": 1}";

      putInCache("default", key, keyContentType, value, APPLICATION_JSON_TYPE);
      ContentResponse response = get("default", key, keyContentType, TEXT_PLAIN_TYPE);

      ResponseAssertion.assertThat(response).hasReturnedText(value);
   }

   @Test
   public void testIntKeysTextToXMLValues() throws Exception {
      Integer key = 12345;
      String keyContentType = "application/x-java-object;type=java.lang.Integer";
      String value = "<foo>bar</foo>";

      putInCache("default", key, keyContentType, value, TEXT_PLAIN_TYPE);
      ContentResponse response = get("default", key, keyContentType, APPLICATION_XML_TYPE);

      ResponseAssertion.assertThat(response).hasReturnedText(value);
   }

   @Test
   public void testInvalidXMLConversion() throws Exception {
      String key = "invalid-xml-key";
      String invalidXML = "foo";

      putInCache("default", key, invalidXML, TEXT_PLAIN_TYPE);

      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", key))
            .header(HttpHeader.ACCEPT, APPLICATION_XML_TYPE)
            .method(HttpMethod.GET).send();

      ResponseAssertion.assertThat(response).containsReturnedText("<string>foo</string>");
   }

}
