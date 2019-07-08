package org.infinispan.rest.resources;

import static org.eclipse.jetty.http.HttpHeader.CONTENT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.testng.AssertJUnit.assertEquals;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Test(groups = "functional", testName = "rest.CacheV2ResourceTest")
public class CacheV2ResourceTest extends AbstractRestResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      cm.defineConfiguration("default", getDefaultCacheBuilder().build());
   }

   @Test
   public void testCacheV2KeyOps() throws Exception {
      String urlWithoutCM = String.format("http://localhost:%d/rest/v2/caches/default", restServer().getPort());

      ContentResponse response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.POST).content(new StringContentProvider("value")).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.POST).content(new StringContentProvider("value")).send();
      ResponseAssertion.assertThat(response).isConflicted();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.PUT).content(new StringContentProvider("value-new")).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).hasReturnedText("value-new");

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.HEAD).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasNoContent();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.DELETE).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(urlWithoutCM + "/key").method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void testCacheV2LifeCycle() throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/caches/", restServer().getPort());

      String xml = getResourceAsString("cache.xml", getClass().getClassLoader());
      String json = getResourceAsString("cache.json", getClass().getClassLoader());

      ContentResponse response = client.newRequest(url + "cache1").header("Content-type", APPLICATION_XML_TYPE)
            .method(HttpMethod.POST).content(new StringContentProvider(xml)).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(url + "cache2").header("Content-type", APPLICATION_JSON_TYPE)
            .method(HttpMethod.POST).content(new StringContentProvider(json)).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(url + "cache1/config").method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).bodyNotEmpty();
      String cache1Cfg = response.getContentAsString();


      response = client.newRequest(url + "cache2/config").method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).bodyNotEmpty();
      String cache2Cfg = response.getContentAsString();

      assertEquals(cache1Cfg, cache2Cfg);

      response = client.newRequest(url + "cache1").method(HttpMethod.DELETE).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(url + "cache1/config").method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void testCacheV2Stats() throws Exception {
      ObjectMapper objectMapper = new ObjectMapper();
      String cacheJson = "{ \"distributed-cache\" : { \"statistics\":true } }";
      String cacheURL = String.format("http://localhost:%d/rest/v2/caches/statCache", restServer().getPort());

      String url = String.format(cacheURL, restServer().getPort());
      ContentResponse response = client.newRequest(url)
            .method(HttpMethod.POST)
            .header(CONTENT_TYPE, APPLICATION_JSON_TYPE)
            .content(new StringContentProvider(cacheJson))
            .send();
      ResponseAssertion.assertThat(response).isOk();

      putStringValueInCache("statCache", "key1", "data");
      putStringValueInCache("statCache", "key2", "data");

      response = client.newRequest(cacheURL + "/stats").send();
      ResponseAssertion.assertThat(response).isOk();

      JsonNode jsonNode = objectMapper.readTree(response.getContent());
      assertEquals(jsonNode.get("currentNumberOfEntries").asInt(), 2);
      assertEquals(jsonNode.get("stores").asInt(), 2);

      response = client.newRequest(cacheURL + "?action=clear").send();
      ResponseAssertion.assertThat(response).isOk();
      response = client.newRequest(cacheURL + "/stats").send();
      ResponseAssertion.assertThat(response).isOk();
      assertEquals(objectMapper.readTree(response.getContent()).get("currentNumberOfEntries").asInt(), 0);
   }

}
