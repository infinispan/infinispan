package org.infinispan.rest.search;

import static org.testng.AssertJUnit.assertEquals;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Tests for search over rest for indexed caches.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.search.IndexedRestSearchTest")
public class IndexedRestSearchTest extends BaseRestSearchTest {

   @Override
   protected ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configurationBuilder.indexing()
                          .enable()
                          .addIndexedEntity("org.infinispan.rest.search.entity.Person")
                          .addProperty("default.directory_provider", "local-heap");
      return configurationBuilder;
   }

   @Test
   public void testReplaceIndexedDocument() throws Exception {
      put(10, createPerson(0, "P", "", "?", "?", "MALE").toString());
      put(10, createPerson(0, "P", "Surname", "?", "?", "MALE").toString());

      ContentResponse response = get("10", "application/json");

      JsonNode person = MAPPER.readTree(response.getContentAsString());
      assertEquals("Surname", person.get("surname").asText());
   }

   @Test
   public void testCORS() throws Exception {
      RestServerHelper server = pickServer();
      String searchUrl = getUrl(server);
      ContentResponse preFlight = client.newRequest(searchUrl)
            .method(HttpMethod.OPTIONS)
            .header(HttpHeader.HOST, "localhost")
            .header(HttpHeader.ORIGIN, "http://localhost:" + server.getPort())
            .header("access-control-request-method", "GET")
            .send();

      ResponseAssertion.assertThat(preFlight).isOk();
      ResponseAssertion.assertThat(preFlight).hasNoContent();
      ResponseAssertion.assertThat(preFlight).containsAllHeaders("access-control-allow-origin", "access-control-allow-methods");
   }
}
