package org.infinispan.rest;

import static org.infinispan.rest.JSONConstants.TYPE;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
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
      object.encoding().key().mediaType(MediaType.TEXT_PLAIN_TYPE);
      object.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);

      restServer.defineCache("objectCache", object);
   }

   @Test
   public void shouldConvertExistingSerializableObjectToJson() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("objectCache", "test", testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "objectCache", "test"))
            .header(HttpHeader.ACCEPT, "application/json")
            .send();

      //then
      System.out.println(response.getContentAsString());
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      ResponseAssertion.assertThat(response).hasReturnedText("{\"" + TYPE + "\":\"" + TestClass.class.getName() + "\",\"name\":\"test\"}");
   }

   @Test
   public void shouldConvertExistingSerializableObjectToXml() throws Exception {
      //given
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("objectCache", "test", testClass);

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
      TestClass testClass = new TestClass();
      testClass.setName("test");
      putValueInCache("compatCache", "test", testClass);

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "compatCache", "test"))
            .header(HttpHeader.ACCEPT, MediaType.APPLICATION_OCTET_STREAM_TYPE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
   }

   @Test
   public void shouldReadTextWithCompatMode() throws Exception {
      //given
      putValueInCache("compatCache", "k1", "v1");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "compatCache", "k1"))
            .header(HttpHeader.ACCEPT, MediaType.APPLICATION_OCTET_STREAM_TYPE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
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
            .header(HttpHeader.ACCEPT, MediaType.APPLICATION_OCTET_STREAM_TYPE)
            .send();

      //then
      System.out.println(response.getContentAsString());
      ResponseAssertion.assertThat(response).hasReturnedBytes("v1".getBytes());
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
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
   public void shouldWriteTextContentWithCompatMode() throws Exception {
      //given
      putStringValueInCache("compatCache", "key1", "data");

      //when
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer.getPort(), "compatCache", "key1"))
            .header(HttpHeader.ACCEPT, MediaType.TEXT_PLAIN_TYPE)
            .send();

      //then
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasReturnedText("data");
      ResponseAssertion.assertThat(response).hasContentType(MediaType.TEXT_PLAIN_TYPE);
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
      ResponseAssertion.assertThat(response).hasContentType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
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

}
