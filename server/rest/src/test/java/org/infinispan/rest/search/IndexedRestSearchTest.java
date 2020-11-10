package org.infinispan.rest.search;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpHeaderNames.ORIGIN;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

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
            .storage(LOCAL_HEAP)
            .addIndexedEntity("org.infinispan.rest.search.entity.Person");
      return configurationBuilder;
   }

   @Test
   public void testReplaceIndexedDocument() throws Exception {
      put(10, createPerson(0, "P", "", "?", "?", "MALE").toString());
      put(10, createPerson(0, "P", "Surname", "?", "?", "MALE").toString());

      RestResponse response = join(get("10", "application/json"));

      Json person = Json.read(response.getBody());
      assertEquals("Surname", person.at("surname").asString());
   }

   @Test
   public void testCORS() {
      String searchUrl = getPath();

      Map<String, String> headers = new HashMap<>();
      headers.put(HOST.toString(), "localhost");
      headers.put(ORIGIN.toString(), "http://localhost:" + pickServer().getPort());
      headers.put(ACCESS_CONTROL_REQUEST_METHOD.toString(), "GET");
      RestResponse preFlight = join(client.raw().options(searchUrl, headers));

      ResponseAssertion.assertThat(preFlight).isOk();
      ResponseAssertion.assertThat(preFlight).hasNoContent();
      ResponseAssertion.assertThat(preFlight).containsAllHeaders("access-control-allow-origin", "access-control-allow-methods");
   }
}
