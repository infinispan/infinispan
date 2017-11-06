package org.infinispan.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
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
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional")
public abstract class BaseRestOperationsTest {
   protected HttpClient client;
   protected RestServerHelper restServer;

   protected abstract ConfigurationBuilder getDefaultCacheBuilder();

   @BeforeClass
   public void beforeSuite() throws Exception {
      restServer = RestServerHelper.defaultRestServer(getDefaultCacheBuilder(), "default");
      defineCaches();
      restServer.start();
      client = new HttpClient();
      client.start();
   }

   protected void defineCaches() {
      ConfigurationBuilder expirationConfiguration = getDefaultCacheBuilder();
      expirationConfiguration.expiration().lifespan(100).maxIdle(100);

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
      text.encoding().key().mediaType(MediaType.TEXT_PLAIN_TYPE);
      text.encoding().value().mediaType(MediaType.TEXT_PLAIN_TYPE);

      ConfigurationBuilder compat = getDefaultCacheBuilder();
//      compat.encoding().value().mediaType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
      compat.compatibility().enable();

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
   public InternalCacheEntry<String, byte[]> getCacheEntry(String cacheName, String key) {
      CacheEntry cacheEntry = getCache(cacheName).getCacheEntry(key);
      return (InternalCacheEntry<String, byte[]>) cacheEntry;
   }

   public AdvancedCache getCache(String cacheName) {
      return restServer.getCacheManager().getCache(cacheName, false).getAdvancedCache().withKeyEncoding(getKeyEncoding());
   }

   protected Class<? extends Encoder> getKeyEncoding() {
      return IdentityEncoder.class;
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
      putStringValueInCache("json", "test", "{\"name\": \"test\"}");

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
      ResponseAssertion.assertThat(response).hasContentType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldGetOctetStreamValueStoredInSpecificFormat() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putBinaryValueInCache("serialized", "test", convertToBytes(testClass), MediaType.APPLICATION_SERIALIZED_OBJECT);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "serialized", "test"))
            .send();

      TestClass convertedObject = convertFromBytes(response.getContent(), TestClass.class);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(MediaType.APPLICATION_SERIALIZED_OBJECT.toString());
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

   protected void putValueInCache(String cacheName, String key, Object testValue) {
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

   protected void putStringValueInCache(String cacheName, String key, String value) throws InterruptedException, ExecutionException, TimeoutException {
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), cacheName, key))
            .content(new StringContentProvider(value))
            .header("Content-type", "text/plain; charset=utf-8")
            .method(HttpMethod.PUT)
            .send();

      ResponseAssertion.assertThat(response).isOk();
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
      putBinaryValueInCache("serialized", "test", convertToBytes(42), MediaType.APPLICATION_SERIALIZED_OBJECT);

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

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("default", "test");

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
      ResponseAssertion.assertThat(getResponse).hasReturnedText("Hey!");

   }

   @Test
   public void shouldPutUnknownFormatValueInCache() throws Exception {
      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "unknown", "test"))
            .content(new StringContentProvider("Hey!"))
            .header("Content-type", "application/unknown")
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("unknown", "test");

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

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("serialized", "test");

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

      String valueFromCache = new String(getCacheEntry("default", "test").getValue());

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

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("expiration", "test");

      Metadata metadata = cacheEntry.getMetadata();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(metadata.lifespan()).isEqualTo(100);
      Assertions.assertThat(metadata.maxIdle()).isEqualTo(100);
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

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("expiration", "test");
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

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("expiration", "test");
      Metadata metadata = cacheEntry.getMetadata();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(metadata.lifespan()).isEqualTo(100);
      Assertions.assertThat(metadata.maxIdle()).isEqualTo(100);
   }

   @Test
   public void testErrorPropagation() throws Exception {
      putStringValueInCache("xml", "key", "value");

      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "xml", "key"))
            .header(HttpHeader.ACCEPT, "application/json")
            .method(HttpMethod.GET)
            .send();

      ResponseAssertion.assertThat(response).isError();
      ResponseAssertion.assertThat(response)
            .hasReturnedText("ISPN000492: Cannot find transcoder between 'application/json' to 'application/xml'");
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

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("expiration", "test");
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

      InternalCacheEntry<String, byte[]> cacheEntry = getCacheEntry("binary", "test");

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
}
