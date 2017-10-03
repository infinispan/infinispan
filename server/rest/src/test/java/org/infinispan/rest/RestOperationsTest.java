package org.infinispan.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.operations.CacheOperationsHelper;
import org.infinispan.rest.operations.mime.MimeMetadata;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.RestOperationsTest")
public class RestOperationsTest extends AbstractInfinispanTest {

   private HttpClient client;
   private RestServerHelper restServer;

   public ConfigurationBuilder getDefaultCacheBuilder() {
      return new ConfigurationBuilder();
   }

   @BeforeClass
   public void beforeSuite() throws Exception {
      restServer = RestServerHelper.defaultRestServer("default");
      ConfigurationBuilder configuration = getDefaultCacheBuilder();
      configuration.expiration().lifespan(100).maxIdle(100);
      restServer.defineCache("expiration", configuration);
      restServer.start();

      client = new HttpClient();
      client.start();
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
      putValueWithMetadataInCache("default", "test", "test", "text/plain", Optional.empty(), Optional.empty());

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
      putValueWithMetadataInCache("default", "test", "test", "text/plain", Optional.empty(), Optional.empty());

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
      putValueWithMetadataInCache("default", "test", "test", "text/plain", Optional.empty(), Optional.empty());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "text/plain")
            .send();

      //then
      ResponseAssertion.assertThat(response).hasEtag();
      ResponseAssertion.assertThat(response).hasHeaderMatching("ETag", "text/plain-\\d+");
   }

   @Test
   public void shouldReturnExtendedHeaders() throws Exception {
      //given
      putValueWithMetadataInCache("default", "test", "test", "text/plain", Optional.empty(), Optional.empty());

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
      putValueWithMetadataInCache("default", "test", "test", "text/plain;charset=UTF-8", Optional.empty(), Optional.empty());

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
      putValueWithMetadataInCache("default", "test", "{\"name\": \"test\"}", "application/json", Optional.empty(), Optional.empty());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
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
      putValueWithMetadataInCache("default", "test", "<xml><name>test</name></xml>", "application/xml", Optional.empty(), Optional.empty());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
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
      putValueWithMetadataInCache("default", "test", "test", "application/unknown", Optional.empty(), Optional.empty());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/unknown");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldGetOctetStreamValueStoredInSpecificFormat() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("default", "test", convertToBytes(testClass));

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "application/octet-stream")
            .send();

      TestClass convertedObject = convertFromBytes(response.getContent(), TestClass.class);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/octet-stream");
      ResponseAssertion.assertThat(response).hasNoCharset();
      Assertions.assertThat(convertedObject.getName()).isEqualTo("test");
   }

   @Test
   public void shouldConvertExistingObjectToText() throws Exception {
      //given
      putValueInCache("default", "test", "test");

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
      putValueInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "text/plain;charset=utf-8")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain;charset=UTF-8");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldConvertExistingSerializableObjectToJson() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("default", "test", testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "application/json")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      ResponseAssertion.assertThat(response).hasReturnedText("{\"name\":\"test\"}");
   }

   @Test
   public void shouldConvertExistingSerializableObjectToXml() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("default", "test", testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header(HttpHeader.ACCEPT, "application/xml")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/xml");
      ResponseAssertion.assertThat(response).hasReturnedText(
            "<org.infinispan.rest.RestOperationsTest_-TestClass>\n" +
            "  <name>test</name>\n" +
            "</org.infinispan.rest.RestOperationsTest_-TestClass>");
   }

   @Test
   public void shouldGetExistingValueWithoutOutputUsingHEAD() throws Exception {
      //given
      putValueWithMetadataInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .method(HttpMethod.HEAD)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasNoContent();
   }

   private void putValueWithMetadataInCache(String cacheName, String key, String testValue, String dataType, Optional<Long> ttl, Optional<Long> idleTime) {
      Metadata metadata = CacheOperationsHelper.createMetadata(restServer.getCacheManager().getCacheConfiguration(cacheName), dataType, ttl, idleTime);
      restServer.getCacheManager().getCache(cacheName).getAdvancedCache().put(key, testValue.getBytes(), metadata);
   }

   private void putValueWithMetadataInCache(String cacheName, String key, String testValue) {
      putValueWithMetadataInCache(cacheName, key, testValue, "text/plain", Optional.empty(), Optional.empty());
   }

   private void putValueInCache(String cacheName, String key, Object testValue) {
      restServer.getCacheManager().getCache(cacheName).getAdvancedCache().put(key, testValue);
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
      putValueWithMetadataInCache("default", "test", "test");

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
   public void shouldDeleteNonExistingValue() throws Exception {
      putValueWithMetadataInCache("default", "test", "test");

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
      putValueWithMetadataInCache("default", "test", "test");

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
      putValueInCache("default", "key1", "test1".getBytes());
      putValueInCache("default", "key2", "test2".getBytes());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "default"))
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
      putValueInCache("default", "key1", "test1".getBytes());
      putValueInCache("default", "key2", "test2".getBytes());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "default"))
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
      putValueInCache("default", "key1", "test1".getBytes());
      putValueInCache("default", "key2", "test2".getBytes());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "default"))
            .header(HttpHeader.ACCEPT, "text/plain; charset=ISO-8859-1")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain; charset=ISO-8859-1");
      ResponseAssertion.assertThat(response).hasReturnedText("key1\nkey2");
   }

   @Test
   public void shouldGetAllEntriesConvertedToHtml() throws Exception {
      //given
      putValueInCache("default", "key1", "test1".getBytes());
      putValueInCache("default", "key2", "test2".getBytes());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "default"))
            .header(HttpHeader.ACCEPT, "text/html")
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/html");
      ResponseAssertion.assertThat(response).hasReturnedText("<html><body><a href=\"default/key1\">key1</a><br/><a href=\"default/key2\">key2</a></body></html>");
   }

   @Test
   public void shouldGetAllEntriesConvertedToJson() throws Exception {
      //given
      putValueInCache("default", "key1", "test1".getBytes());
      putValueInCache("default", "key2", "test2".getBytes());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "default"))
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
      putValueInCache("default", "key1", "test1".getBytes());
      putValueInCache("default", "key2", "test2".getBytes());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s", restServer.getPort(), "default"))
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
      putValueWithMetadataInCache("default", "key1", "test1", "text/plain",Optional.empty(), Optional.empty());

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "key1"))
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
      putValueWithMetadataInCache("default", "key1", "test1", "text/plain",Optional.empty(), Optional.empty());

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
      putValueWithMetadataInCache("default", "test", "test");

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
      putValueWithMetadataInCache("default", "test", "test");

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

      InternalCacheEntry<String, byte[]> cacheEntry = (InternalCacheEntry<String, byte[]>) restServer.getCacheManager()
            .<String, byte[]>getCache("default", false)
            .getAdvancedCache()
            .getCacheEntry("test");
      MimeMetadata metadata = ((MimeMetadata) cacheEntry.getMetadata());

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasEtag();
      Assertions.assertThat(new String(cacheEntry.getValue())).isEqualTo("Hey!");
      Assertions.assertThat(metadata.contentType()).isEqualTo("text/plain;charset=UTF-8");
   }

   @Test
   public void shouldPutUnknownFormatValueInCache() throws Exception {
      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .content(new StringContentProvider("Hey!"))
            .header("Content-type", "application/unknown")
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = (InternalCacheEntry<String, byte[]>) restServer.getCacheManager()
            .<String, byte[]>getCache("default", false)
            .getAdvancedCache()
            .getCacheEntry("test");
      MimeMetadata metadata = ((MimeMetadata) cacheEntry.getMetadata());

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasEtag();
      Assertions.assertThat(new String(cacheEntry.getValue())).isEqualTo("Hey!");
      Assertions.assertThat(metadata.contentType()).isEqualTo("application/unknown");
   }

   @Test
   public void shouldPutSerializedValueInCache() throws Exception {
      //when
      TestClass testClass = new TestClass();
      testClass.setName("test");

      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .content(new BytesContentProvider(convertToBytes(testClass)))
            .header("Content-type", "application/octet-stream")
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = (InternalCacheEntry<String, byte[]>) restServer.getCacheManager()
            .<String, byte[]>getCache("default", false)
            .getAdvancedCache()
            .getCacheEntry("test");
      MimeMetadata metadata = ((MimeMetadata) cacheEntry.getMetadata());

      TestClass valueFromCache = convertFromBytes(cacheEntry.getValue(), TestClass.class);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasEtag();
      Assertions.assertThat(valueFromCache.getName()).isEqualTo("test");
      Assertions.assertThat(metadata.contentType()).isEqualTo("application/octet-stream");
   }

   @Test
   public void shouldConflictWhenTryingToReplaceExistingEntryUsingPost() throws Exception {
      //given
      putValueWithMetadataInCache("default", "test", "test");

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
      putValueWithMetadataInCache("default", "test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .content(new StringContentProvider("Hey!"))
            .header("Content-type", "text/plain;charset=UTF-8")
            .method(HttpMethod.PUT)
            .send();
      String valueFromCache = new String(restServer.getCacheManager()
            .<String, byte[]>getCache("default", false)
            .getAdvancedCache()
            .getCacheEntry("test").getValue());

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
   public void shouldSupportCompatibilityWithCustomClass() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");

      restServer.getCacheManager().getCache("default").getAdvancedCache().put("test", testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header("Content-type", "text/plain;charset=UTF-8")
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain;charset=UTF-8");
      ResponseAssertion.assertThat(response).hasReturnedText("TestClass{name='test'}");
   }

   @Test
   public void shouldSupportCompatibilityWithStrings() throws Exception {
      //given
      restServer.getCacheManager().getCache("default").getAdvancedCache().put("test", "test");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "default", "test"))
            .header("Content-type", "text/plain;charset=UTF-8")
            .method(HttpMethod.GET)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain;charset=UTF-8");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldPutEntryWithDefaultTllAndIdleTime() throws Exception {
      //when
      ContentResponse response = client
            .POST(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "expiration", "test"))
            .content(new StringContentProvider("test"))
            .send();

      InternalCacheEntry<String, byte[]> cacheEntry = (InternalCacheEntry<String, byte[]>) restServer.getCacheManager()
            .<String, byte[]>getCache("expiration", false)
            .getAdvancedCache()
            .getCacheEntry("test");
      MimeMetadata metadata = ((MimeMetadata) cacheEntry.getMetadata());

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

      InternalCacheEntry<String, byte[]> cacheEntry = (InternalCacheEntry<String, byte[]>) restServer.getCacheManager()
            .<String, byte[]>getCache("expiration", false)
            .getAdvancedCache()
            .getCacheEntry("test");
      MimeMetadata metadata = ((MimeMetadata) cacheEntry.getMetadata());

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

      InternalCacheEntry<String, byte[]> cacheEntry = (InternalCacheEntry<String, byte[]>) restServer.getCacheManager()
            .<String, byte[]>getCache("expiration", false)
            .getAdvancedCache()
            .getCacheEntry("test");
      MimeMetadata metadata = ((MimeMetadata) cacheEntry.getMetadata());

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(metadata.lifespan()).isEqualTo(100);
      Assertions.assertThat(metadata.maxIdle()).isEqualTo(100);
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

      InternalCacheEntry<String, byte[]> cacheEntry = (InternalCacheEntry<String, byte[]>) restServer.getCacheManager()
            .<String, byte[]>getCache("expiration", false)
            .getAdvancedCache()
            .getCacheEntry("test");
      MimeMetadata metadata = ((MimeMetadata) cacheEntry.getMetadata());

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(metadata.lifespan()).isEqualTo(50 * 1000);
      Assertions.assertThat(metadata.maxIdle()).isEqualTo(50 * 1000);
   }

   static class TestClass implements Serializable {

      private String name;

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      @Override
      public String toString() {
         return "TestClass{" +
               "name='" + name + '\'' +
               '}';
      }
   }
}
