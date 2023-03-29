package org.infinispan.quarkus.server;

import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * @author Ryan Emerson
 * @since 11.0
 */
public class RestCacheManagerResource {

   @ClassRule
   public static final InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(2)
               .property("infinispan.bind.address", "0.0.0.0")
               .build();
   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final ObjectMapper mapper = new ObjectMapper();

   @Test
   public void testHealthStatus() throws Exception {
      RestResponse restResponse = sync(client().healthStatus());
      assertEquals(200, restResponse.getStatus());
      assertEquals("HEALTHY", restResponse.getBody());
   }

   @Test
   public void testHealthInfo() throws Exception {
      RestResponse restResponse = sync(client().health());
      assertEquals(200, restResponse.getStatus());

      JsonNode root = mapper.readTree(restResponse.getBody());

      JsonNode clusterHealth = root.get("cluster_health");
      assertEquals(RestCacheManagerResource.class.getName(), clusterHealth.get("cluster_name").asText());
      assertEquals("HEALTHY", clusterHealth.get("health_status").asText());
      assertEquals(2, clusterHealth.get("number_of_nodes").asInt());
      assertEquals(2, clusterHealth.withArray("node_names").size());

      ArrayNode cacheHealth = root.withArray("cache_health");
      cacheHealth.forEach(cache -> {
         assertEquals("HEALTHY", cache.get("status").asText());
         assertNotNull(cache.get("cache_name"));
      });
   }

   @Test
   public void testNamedCacheConfiguration() throws Exception {
      RestResponse restResponse = sync(client().cacheConfigurations());
      assertEquals(200, restResponse.getStatus());
      ArrayNode configArray = (ArrayNode) mapper.readTree(restResponse.getBody());
      configArray.forEach(config -> {
         assertNotNull(config.get("name"));
         assertNotNull(config.get("configuration"));
      });
   }

   @Test
   public void testCacheInfo() throws Exception {
      RestClient client = SERVER_TEST.rest().create();
      String cacheName = "test";
      RestResponse restResponse = sync(client.cache(cacheName).createWithTemplate("org.infinispan.LOCAL"));
      assertEquals(200, restResponse.getStatus());

      restResponse = sync(client.raw().get("/rest/v2/cache-managers/default/caches"));
      assertEquals(200, restResponse.getStatus());
      ArrayNode cacheArray = (ArrayNode) mapper.readTree(restResponse.getBody());
      assertNotNull(cacheArray);
      cacheArray.forEach(cache -> {
         assertEquals("RUNNING", cache.get("status").asText());
         assertEquals("HEALTHY", cache.get("health").asText());

         if (cacheName.equals(cache.get("name").asText())) {
            assertEquals("local-cache", cache.get("type").asText());
            assertFalse(cache.get("simple_cache").asBoolean());
            assertFalse(cache.get("transactional").asBoolean());
            assertFalse(cache.get("persistent").asBoolean());
            assertFalse(cache.get("bounded").asBoolean());
            assertFalse(cache.get("indexed").asBoolean());
            assertFalse(cache.get("secured").asBoolean());
            assertFalse(cache.get("has_remote_backup").asBoolean());
         }
      });
   }

   private RestCacheManagerClient client() {
      return SERVER_TEST.rest().create().cacheManager("default");
   }
}
