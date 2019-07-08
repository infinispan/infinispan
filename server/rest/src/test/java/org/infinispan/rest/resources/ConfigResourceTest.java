package org.infinispan.rest.resources;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.testng.AssertJUnit.assertEquals;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Test(groups = "functional", testName = "rest.ConfigResourceTest")
public class ConfigResourceTest extends AbstractRestResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      ConfigurationBuilder object = getDefaultCacheBuilder();
      object.encoding().key().mediaType(TEXT_PLAIN_TYPE);
      object.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);

      ConfigurationBuilder legacyStorageCache = getDefaultCacheBuilder();
      legacyStorageCache.encoding().key().mediaType("application/x-java-object;type=java.lang.String");

      cm.defineConfiguration("objectCache", object.build());
   }


   @Test
   public void testGetExistingConfig() throws Exception {
      String URL = "http://localhost:%d/rest/v2/configurations/objectCache";
      ContentResponse response = client.newRequest(String.format(URL, restServer().getPort())).send();

      ResponseAssertion.assertThat(response).isOk();

      JsonNode jsonNode = new ObjectMapper().readTree(response.getContentAsString());

      assertEquals(APPLICATION_OBJECT_TYPE, jsonNode
            .path("distributed-cache").path("encoding").path("value").path("media-type").asText());
   }

   @Test
   public void testConfigConverter() throws Exception {
      ContentResponse response = client.newRequest(String.format("http://localhost:%d/rest/v2/configurations?action=toJSON", restServer().getPort()))
            .method(HttpMethod.POST)
            .content(new InputStreamContentProvider(getClass().getResourceAsStream("/infinispan.xml"))).send();

      ResponseAssertion.assertThat(response).isOk();

      JsonNode jsonNode = new ObjectMapper().readTree(response.getContentAsString());

      assertEquals(2, jsonNode.findValue("string-keyed-jdbc-store").size());
   }

}
