package org.infinispan.rest.resources;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.MATCH_ALL_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.rest.RequestHeader.ACCEPT_HEADER;
import static org.infinispan.rest.RequestHeader.IF_NONE_MATCH;
import static org.infinispan.rest.RequestHeader.KEY_CONTENT_TYPE_HEADER;
import static org.infinispan.rest.ResponseHeader.MAX_IDLE_TIME_HEADER;
import static org.infinispan.rest.ResponseHeader.TIME_TO_LIVE_HEADER;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.assertj.core.api.Assertions;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestRawClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.ResponseHeader;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.rest.TestClass;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional")
public abstract class BaseCacheResourceTest extends AbstractRestResourceTest {

   private void defineCounters(EmbeddedCacheManager cm) {
      CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cm);
      counterManager.defineCounter("weak", CounterConfiguration.builder(CounterType.WEAK).build());
      counterManager.defineCounter("strong", CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
   }

   private static final long DEFAULT_LIFESPAN = 1859446;
   private static final long DEFAULT_MAX_IDLE = 45190;

   private static final long DEFAULT_LIFESPAN_SECONDS = DEFAULT_LIFESPAN / 1000;
   private static final long DEFAULT_MAX_IDLE_SECONDS = DEFAULT_MAX_IDLE / 1000;

   protected void defineCaches(EmbeddedCacheManager cm) {
      defineCounters(cm);
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
      javaSerialized.encoding().value().mediaType(APPLICATION_SERIALIZED_OBJECT_TYPE);

      ConfigurationBuilder text = getDefaultCacheBuilder();
      text.encoding().key().mediaType(TEXT_PLAIN_TYPE);
      text.encoding().value().mediaType(TEXT_PLAIN_TYPE);

      ConfigurationBuilder pojoCache = getDefaultCacheBuilder();
      pojoCache.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      pojoCache.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);

      cm.defineConfiguration("default", getDefaultCacheBuilder().build());
      cm.defineConfiguration("expiration", expirationConfiguration.build());
      cm.defineConfiguration("xml", xmlCacheConfiguration.build());
      cm.defineConfiguration("json", jsonCacheConfiguration.build());
      cm.defineConfiguration("binary", octetStreamCacheConfiguration.build());
      cm.defineConfiguration("unknown", unknownContentCacheConfiguration.build());
      cm.defineConfiguration("serialized", javaSerialized.build());
      cm.defineConfiguration("textCache", text.build());
      cm.defineConfiguration("pojoCache", pojoCache.build());
   }

   @Test
   public void shouldGetNonExistingValue() {
      CompletionStage<RestResponse> response = client.cache("default").get("nonExisting");

      ResponseAssertion.assertThat(response).doesntExist();
   }

   @Test
   public void shouldReturnNotExistingOnWrongContext() {
      putStringValueInCache("default", "test", "test");

      RestRawClient rawClient = client.raw();
      String path = String.format("/wrongContext/%s/%s", "default", "test");

      CompletionStage<RestResponse> response = rawClient.get(path);

      //then
      ResponseAssertion.assertThat(response).doesntExist();
   }

   @Test
   public void shouldGetAsciiValueStoredInSpecificFormat() {
      putStringValueInCache("default", "test", "test");

      CompletionStage<RestResponse> response = client.cache("default").get("test", TEXT_PLAIN_TYPE);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldHaveProperEtagWhenGettingValue() {
      //given
      putStringValueInCache("default", "test", "test");

      CompletionStage<RestResponse> response = client.cache("default").get("test", TEXT_PLAIN_TYPE);

      //then
      ResponseAssertion.assertThat(response).hasEtag();
      ResponseAssertion.assertThat(response).hasHeaderMatching("ETag", "-\\d+");
   }

   @Test
   public void shouldReturnExtendedHeaders() {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").get("test", TEXT_PLAIN_TYPE, true);

      //then
      ResponseAssertion.assertThat(response).hasExtendedHeaders();
   }

   @Test
   public void shouldGetUtf8ValueStoredInSpecificFormat() {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").get("test", "text/plain;charset=UTF-8");

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain;charset=UTF-8");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldGetJsonValueStoredInSpecificFormat() {
      //given
      putJsonValueInCache("json", "test", "{\"name\": \"test\"}");

      //when
      CompletionStage<RestResponse> response = client.cache("json").get("test", APPLICATION_JSON_TYPE);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      ResponseAssertion.assertThat(response).hasReturnedText("{\"name\": \"test\"}");
   }

   @Test
   public void shouldGetXmlValueStoredInSpecificFormat() {
      //given
      putStringValueInCache("xml", "test", "<xml><name>test</name></xml>");

      //when
      CompletionStage<RestResponse> response = client.cache("xml").get("test", APPLICATION_XML_TYPE);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/xml");
      ResponseAssertion.assertThat(response).hasReturnedText("<xml><name>test</name></xml>");
   }

   @Test
   public void shouldGetValueStoredInUnknownFormat() {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").get("test");

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
      RestResponse response = join(client.cache("serialized").get("test", Map.of(ACCEPT_HEADER.toString(), APPLICATION_SERIALIZED_OBJECT_TYPE)));

      TestClass convertedObject = convertFromBytes(response.bodyAsByteArray());

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(APPLICATION_SERIALIZED_OBJECT.toString());
      ResponseAssertion.assertThat(response).hasNoCharset();
      Assertions.assertThat(convertedObject.getName()).isEqualTo("test");
   }

   @Test
   public void shouldConvertExistingObjectToText() {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").get("test", TEXT_PLAIN_TYPE);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldConvertExistingObjectToTextUtf8() {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").get("test", "text/plain;charset=UTF-8");

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("text/plain");
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldGetExistingValueWithoutOutputUsingHEAD() {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").head("test");

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasNoContent();
   }

   private void putInCache(String cacheName, Object key, String keyContentType, String value, String contentType) {
      RestEntity entity = RestEntity.create(MediaType.fromString(contentType), value);
      CompletionStage<RestResponse> response = client.cache(cacheName).put(key.toString(), keyContentType, entity);

      ResponseAssertion.assertThat(response).isOk();
   }

   private byte[] convertToBytes(Object object) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
           ObjectOutput out = new ObjectOutputStream(bos)) {
         out.writeObject(object);
         return bos.toByteArray();
      }
   }

   private <T> T convertFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
         ObjectInputStream in = new ObjectInputStream(bis);
         return (T) in.readObject();
      }
   }

   @Test
   public void shouldDeleteExistingValue() throws Exception {
      putStringValueInCache("default", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").remove("test");

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(restServer().getCacheManager().getCache("default")).isEmpty();
   }

   @Test
   public void shouldDeleteExistingValueWithAcceptHeader() throws Exception {
      putBinaryValueInCache("serialized", "test", convertToBytes(42), APPLICATION_SERIALIZED_OBJECT);

      Map<String, String> headers = createHeaders(ACCEPT_HEADER, APPLICATION_SERIALIZED_OBJECT_TYPE);

      CompletionStage<RestResponse> headResponse = client.cache("serialized").head("test", headers);

      ResponseAssertion.assertThat(headResponse).isOk();
      ResponseAssertion.assertThat(headResponse).hasContentType("application/x-java-serialized-object");

      headers = createHeaders(ACCEPT_HEADER, "text/plain;charset=UTF-8");

      CompletionStage<RestResponse> response = client.cache("serialized").remove("test", headers);

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(restServer().getCacheManager().getCache("binary")).isEmpty();
   }

   @Test
   public void shouldDeleteNonExistingValue() throws Exception {
      putStringValueInCache("default", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").remove("doesnt_exist");

      //then
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void shouldDeleteEntireCache() throws Exception {
      putStringValueInCache("default", "test", "test");

      //when
      CompletionStage<RestResponse> response = client.cache("default").clear();

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(restServer().getCacheManager().getCache("default")).isEmpty();
   }

   @Test
   public void shouldGetAllEntriesFromEmptyCache() {
      //when
      CompletionStage<RestResponse> response = client.cache("default").keys("text/plain; charset=utf-8");

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasReturnedText("[]");
   }

   @Test
   public void shouldGetAllKeysConvertedToJson() throws Exception {
      //given
      putStringValueInCache("textCache", "key1", "test1");
      putStringValueInCache("textCache", "key2", "test2");

      //when
      CompletionStage<RestResponse> response = client.cache("textCache").keys(APPLICATION_JSON_TYPE);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      // keys can be returned in any order
      ResponseAssertion.assertThat(response).hasReturnedText("[\"key2\",\"key1\"]", "[\"key1\",\"key2\"]");
   }

   @Test
   public void shouldAcceptMultipleAcceptHeaderValues() throws Exception {
      //given
      putStringValueInCache("textCache", "key1", "test1");

      //when
      CompletionStage<RestResponse> response = client.cache("textCache").get("key1", "ignored/wrong , text/plain");

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
      CompletionStage<RestResponse> response = client.cache("default").get("key1", "application/wrong-content-type");

      //then
      ResponseAssertion.assertThat(response).isNotAcceptable();
   }

   @Test
   public void shouldNotAcceptUnknownContentTypeWithHead() throws Exception {
      putStringValueInCache("default", "key1", "test1");

      Map<String, String> headers = createHeaders(ACCEPT_HEADER, "garbage/garbage");

      CompletionStage<RestResponse> response = client.cache("default").head("key1", headers);

      ResponseAssertion.assertThat(response).isNotAcceptable();
   }

   @Test
   public void shouldNotReturnValueIfSendingCorrectETag() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      RestResponse firstResponse = join(client.cache("default").get("test"));

      String etagFromFirstCall = firstResponse.headers().get("ETag").get(0);

      Map<String, String> headers = createHeaders(IF_NONE_MATCH, etagFromFirstCall);

      CompletionStage<RestResponse> secondResponse = client.cache("default").get("test", headers);

      //then
      Assertions.assertThat(etagFromFirstCall).isNotNull().isNotEmpty();
      ResponseAssertion.assertThat(secondResponse).isNotModified();
   }

   @Test
   public void shouldReturnEntityWhenSendingWrongETag() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      Map<String, String> headers = createHeaders(IF_NONE_MATCH, "Invalid-etag");

      //when
      CompletionStage<RestResponse> response = client.cache("default").get("test", headers);

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasReturnedText("test");
   }

   @Test
   public void shouldPutTextValueInCache() {
      //when
      RestEntity restEntity = RestEntity.create(MediaType.fromString("text/plain;charset=UTF-8"), "Hey!");
      CompletionStage<RestResponse> response = client.cache("default").post("test", restEntity);
      ResponseAssertion.assertThat(response).isOk();

      RestResponse getResponse = join(client.cache("default").get("test"));

      //then
      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).hasEtag();
      Assertions.assertThat(getResponse.body()).isEqualTo("Hey!");
   }

   @Test
   public void shouldReturnJsonWithDefaultConfig() throws Exception {
      String value = "\"Hey!\"";
      putStringValueInCache("textCache", "test", value);

      CompletionStage<RestResponse> getResponse = client.cache("textCache").get("test", APPLICATION_JSON_TYPE);

      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).hasReturnedText(value);

   }

   @Test
   public void shouldPutUnknownFormatValueInCache() {
      //when
      RestEntity restEntity = RestEntity.create(MediaType.fromString("application/unknown"), "Hey!");
      CompletionStage<RestResponse> response = client.cache("unknown").post("test", restEntity);
      ResponseAssertion.assertThat(response).isOk();

      RestResponse getResponse = join(client.cache("unknown").get("test"));

      //then
      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).hasEtag();
      Assertions.assertThat(getResponse.body()).isEqualTo("Hey!");
   }

   @Test
   public void shouldPutSerializedValueInCache() throws Exception {
      //when
      TestClass testClass = new TestClass();
      testClass.setName("test");

      CompletionStage<RestResponse> response = client.cache("serialized")
            .post("test", RestEntity.create(APPLICATION_SERIALIZED_OBJECT, convertToBytes(testClass)));
      ResponseAssertion.assertThat(response).isOk();

      RestResponse getResponse = join(client.cache("serialized").get("test", APPLICATION_SERIALIZED_OBJECT_TYPE));
      TestClass valueFromCache = convertFromBytes(getResponse.bodyAsByteArray());

      ResponseAssertion.assertThat(getResponse).hasEtag();
      Assertions.assertThat(valueFromCache.getName()).isEqualTo("test");
   }

   @Test
   public void shouldConflictWhenTryingToReplaceExistingEntryUsingPost() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      RestEntity restEntity = RestEntity.create(MediaType.fromString("text/plain;charset=UTF-8"), "Hey!");

      CompletionStage<RestResponse> response = client.cache("default").post("test", restEntity);

      //then
      ResponseAssertion.assertThat(response).isConflicted();
   }

   @Test
   public void shouldUpdateEntryWhenReplacingUsingPut() throws Exception {
      //given
      putStringValueInCache("default", "test", "test");

      //when
      join(client.cache("default").put("test", "Hey!"));

      RestResponse getResponse = join(client.cache("default").get("test"));

      //then
      ResponseAssertion.assertThat(getResponse).isOk();
      Assertions.assertThat(getResponse.body()).isEqualTo("Hey!");
   }

   @Test
   public void shouldPutEntryWithDefaultTllAndIdleTime() {
      //when
      CompletionStage<RestResponse> response = client.cache("expiration").post("test", "test");
      ResponseAssertion.assertThat(response).isOk();

      RestResponse getResponse = join(client.cache("expiration").get("test"));

      //then
      ResponseAssertion.assertThat(getResponse).isOk();
      Assertions.assertThat(getLifespan(getResponse)).isEqualTo(DEFAULT_LIFESPAN_SECONDS);
      Assertions.assertThat(getMaxIdle(getResponse)).isEqualTo(DEFAULT_MAX_IDLE_SECONDS);
   }

   private Long getLifespan(RestResponse response) {
      return getLongHeader(response, TIME_TO_LIVE_HEADER);
   }

   private Long getMaxIdle(RestResponse response) {
      return getLongHeader(response, MAX_IDLE_TIME_HEADER);
   }

   private Long getLongHeader(RestResponse response, ResponseHeader responseHeader) {
      String header = response.header(responseHeader.getValue());
      if (header == null) {
         return null;
      }
      return Long.parseLong(header);
   }

   @Test
   public void shouldPutImmortalEntryWithMinusOneTtlAndIdleTime() {
      RestCacheClient expirationCache = client.cache("expiration");

      //when
      CompletionStage<RestResponse> response = expirationCache.put("test", "test", -1, -1);
      ResponseAssertion.assertThat(response).isOk();

      RestResponse getResponse = join(expirationCache.get("test"));

      //then
      ResponseAssertion.assertThat(getResponse).isOk();
      Assertions.assertThat(getLifespan(getResponse)).isNull();
      Assertions.assertThat(getMaxIdle(getResponse)).isNull();
   }

   @Test
   public void shouldPutImmortalEntryWithZeroTtlAndIdleTime() {
      RestCacheClient expirationCache = client.cache("expiration");

      //when
      CompletionStage<RestResponse> response = expirationCache.post("test", "test", 0, 0);
      ResponseAssertion.assertThat(response).isOk();

      RestResponse getResponse = join(expirationCache.get("test"));

      //then
      ResponseAssertion.assertThat(getResponse).isOk();
      Assertions.assertThat(getLifespan(getResponse)).isEqualTo(DEFAULT_LIFESPAN_SECONDS);
      Assertions.assertThat(getMaxIdle(getResponse)).isEqualTo(DEFAULT_MAX_IDLE_SECONDS);
   }

   @Test
   public void testErrorPropagation() throws Exception {
      putStringValueInCache("xml", "key", "<value/>");

      CompletionStage<RestResponse> response = client.cache("xml").get("key", APPLICATION_JSON_TYPE);

      ResponseAssertion.assertThat(response).isNotAcceptable();
      ResponseAssertion.assertThat(response).containsReturnedText("Cannot convert to application/json");
   }

   @Test
   public void shouldPutEntryWithTtlAndIdleTime() {
      final RestCacheClient expirationCache = client.cache("expiration");

      //when
      CompletionStage<RestResponse> response = expirationCache.post("test", "test", 50, 30);
      ResponseAssertion.assertThat(response).isOk();

      RestResponse getResponse = join(expirationCache.get("test"));

      //then
      ResponseAssertion.assertThat(getResponse).isOk();
      Assertions.assertThat(getLifespan(getResponse)).isEqualTo(50);
      Assertions.assertThat(getMaxIdle(getResponse)).isEqualTo(30);
   }

   @Test
   public void shouldPutLargeObject() {
      byte[] payload = new byte[1_000_000];
      final RestCacheClient binaryCache = client.cache("binary");
      CompletionStage<RestResponse> response = binaryCache.post("test", RestEntity.create(APPLICATION_OCTET_STREAM, payload));
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasEtag();

      RestResponse getResponse = join(binaryCache.get("test", Map.of("Accept", APPLICATION_OCTET_STREAM_TYPE)));
      Assertions.assertThat(getResponse.bodyAsByteArray().length).isEqualTo(1_000_000);
   }

   @Test
   public void shouldFailTooLargeObject() {
      //when
      byte[] payload = new byte[1_100_000];
      RestEntity restEntity = RestEntity.create(APPLICATION_OCTET_STREAM, payload);

      CompletionStage<RestResponse> response = client.cache("default").post("test", restEntity);

      //then
      ResponseAssertion.assertThat(response).isPayloadTooLarge();
   }

   @Test
   public void testWildcardAccept() throws Exception {
      putStringValueInCache("default", "test", "test");

      CompletionStage<RestResponse> getResponse = client.cache("default").get("test", MATCH_ALL_TYPE);

      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).hasReturnedText("test");

   }

   protected RestResponse get(String cacheName, Object key, String keyContentType, String acceptHeader) {
      Map<String, String> headers = new HashMap<>();
      if (acceptHeader != null) {
         headers.put(ACCEPT_HEADER.toString(), acceptHeader);
      }
      if (keyContentType != null) {
         headers.put(KEY_CONTENT_TYPE_HEADER.toString(), keyContentType);
      }
      RestResponse response = join(client.cache(cacheName).get(key.toString(), headers));
      ResponseAssertion.assertThat(response).isOk();
      return response;
   }

   protected RestResponse get(String cacheName, Object key, String acceptHeader) {
      return get(cacheName, key, null, acceptHeader);
   }

   @Test
   public void shouldAcceptUrlEncodedContentForDefaultCache() throws Exception {
      String value = "word1 word2";
      String urlEncoded = URLEncoder.encode(value, "UTF-8");
      putBinaryValueInCache("default", "test", urlEncoded.getBytes(UTF_8), APPLICATION_WWW_FORM_URLENCODED);

      RestResponse getResponse = get("default", "test", TEXT_PLAIN_TYPE);

      ResponseAssertion.assertThat(getResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(getResponse).hasContentType(TEXT_PLAIN_TYPE);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithoutAccept() throws Exception {
      putStringValueInCache("default", "test", "test");

      RestResponse getResponse = get("default", "test", null);

      ResponseAssertion.assertThat(getResponse).hasReturnedText("test");
      ResponseAssertion.assertThat(getResponse).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithAccept() throws Exception {
      String value = "\"test\"";
      putStringValueInCache("default", "test", value);

      RestResponse jsonResponse = get("default", "test", "application/json");

      ResponseAssertion.assertThat(jsonResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(jsonResponse).hasContentType("application/json");

      RestResponse textResponse = get("default", "test", "text/plain");

      ResponseAssertion.assertThat(textResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(textResponse).hasContentType("text/plain");

      RestResponse binaryResponse = get("default", "test", APPLICATION_OCTET_STREAM_TYPE);

      ResponseAssertion.assertThat(binaryResponse).hasReturnedBytes(value.getBytes(UTF_8));
      ResponseAssertion.assertThat(binaryResponse).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithBinary() throws Exception {
      TestClass testClass = new TestClass();
      Marshaller marshaller = TestingUtil.createProtoStreamMarshaller(RestTestSCI.INSTANCE);
      byte[] javaSerialized = marshaller.objectToByteBuffer(testClass);

      putBinaryValueInCache("default", "test", javaSerialized, APPLICATION_OCTET_STREAM);

      RestResponse response = get("default", "test", APPLICATION_OCTET_STREAM_TYPE);

      ResponseAssertion.assertThat(response).hasContentType(APPLICATION_OCTET_STREAM_TYPE).hasReturnedBytes(javaSerialized);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithWildcardAccept() throws Exception {
      putStringValueInCache("default", "test", "test");

      RestResponse getResponse = get("default", "test", "*/*");

      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).hasReturnedText("test");
      ResponseAssertion.assertThat(getResponse).hasContentType(APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldNegotiateFromDefaultCacheWithMultipleAccept() throws Exception {
      String value = "1432";
      putStringValueInCache("default", "test", value);

      RestResponse sameWeightResponse = get("default", "test", "text/html,application/xhtml+xml,*/*");

      ResponseAssertion.assertThat(sameWeightResponse).isOk();
      ResponseAssertion.assertThat(sameWeightResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(sameWeightResponse).hasContentType(APPLICATION_OCTET_STREAM_TYPE);

      RestResponse weightedResponse = get("default", "test", "text/plain;q=0.1, application/json;q=0.8, */*;q=0.7");

      ResponseAssertion.assertThat(weightedResponse).isOk();
      ResponseAssertion.assertThat(weightedResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(weightedResponse).hasContentType(APPLICATION_JSON_TYPE);
   }

   @Test
   public void shouldNegotiateFromJsonCacheWithoutAccept() throws Exception {
      String cacheName = "json";
      String key = "1";
      String value = "{\"id\": 1}";

      putJsonValueInCache(cacheName, key, value);

      RestResponse getResponse = get(cacheName, key, null);

      ResponseAssertion.assertThat(getResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(getResponse).hasContentType(APPLICATION_JSON_TYPE);
   }

   @Test
   public void shouldNegotiateFromJsonCacheWithAccept() throws Exception {
      String cacheName = "json";
      String key = "1";
      String value = "{\"id\": 1}";

      putJsonValueInCache(cacheName, key, value);

      RestResponse jsonResponse = get(cacheName, key, APPLICATION_JSON_TYPE);

      ResponseAssertion.assertThat(jsonResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(jsonResponse).hasContentType(APPLICATION_JSON_TYPE);

      RestResponse textResponse = get(cacheName, key, TEXT_PLAIN_TYPE);

      ResponseAssertion.assertThat(textResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(textResponse).hasContentType(TEXT_PLAIN_TYPE);
   }

   @Test
   public void shouldNegotiateFromJsonCacheWithWildcardAccept() throws Exception {
      String cacheName = "json";
      String key = "1";
      String value = "{\"id\": 1}";

      putJsonValueInCache(cacheName, key, value);

      RestResponse jsonResponse = get(cacheName, key, "*/*");

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

      RestResponse jsonResponse = get(cacheName, key, "text/html,*/*");

      ResponseAssertion.assertThat(jsonResponse).isOk();
      ResponseAssertion.assertThat(jsonResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(jsonResponse).hasContentType(APPLICATION_JSON_TYPE);

      RestResponse binaryResponse = get(cacheName, key, "application/xml, text/plain; q=0.71, */*;q=0.7");

      ResponseAssertion.assertThat(binaryResponse).isOk();
      ResponseAssertion.assertThat(binaryResponse).hasReturnedText(value);
      ResponseAssertion.assertThat(binaryResponse).hasContentType(TEXT_PLAIN_TYPE);
   }

   @Test
   public void shouldNegotiateOnlySupportedFromDefaultCacheWithMultipleAccept() throws Exception {
      String value = "<test/>";
      putStringValueInCache("default", "test", value);

      RestResponse getResponse = get("default", "test", "text/html, application/xml");

      ResponseAssertion.assertThat(getResponse).hasReturnedText("<test/>");
      ResponseAssertion.assertThat(getResponse).hasContentType("application/xml");
   }

   @Test
   public void shouldHandleInvalidPath() {
      String Url = String.format("/rest/v2/caches/%s", "asdjsad");
      Map<String, String> headers = createHeaders(ACCEPT_HEADER, "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");

      CompletionStage<RestResponse> response = client.raw().get(Url, headers);

      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void shouldHandleIncompletePath() {
      String Url = String.format("/rest/v2/caches/%s?action", "default");

      CompletionStage<RestResponse> response = client.raw().get(Url);

      ResponseAssertion.assertThat(response).isBadRequest();
   }

   @Test
   public void testIntKeysTextToXMLValues() {
      Integer key = 12345;
      String keyContentType = "application/x-java-object;type=java.lang.Integer";
      String value = "<foo>bar</foo>";

      putInCache("default", key, keyContentType, value, TEXT_PLAIN_TYPE);
      RestResponse response = get("default", key, keyContentType, APPLICATION_XML_TYPE);

      ResponseAssertion.assertThat(response).hasReturnedText(value);
   }

   @Test
   public void testInvalidXMLConversion() throws Exception {
      String key = "invalid-xml-key";
      String invalidXML = "foo";

      putInCache("default", key, invalidXML, TEXT_PLAIN_TYPE);

      CompletionStage<RestResponse> response = client.cache("default").get(key, APPLICATION_XML_TYPE);

      ResponseAssertion.assertThat(response).containsReturnedText("<string>foo</string>");
   }

   protected Map<String, String> createHeaders(RequestHeader header, String value) {
      Map<String, String> headers = new HashMap<>();
      headers.put(header.toString(), value);
      return headers;
   }

}
