package org.infinispan.server.functional;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.Sets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * @since 10.0
 */
public class RestServerResource {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final ObjectMapper mapper = new ObjectMapper();

   @Test
   public void testConfig() throws Exception {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().configuration());
      JsonNode configNode = mapper.readTree(restResponse.getBody());

      JsonNode server = configNode.get("server");
      JsonNode interfaces = server.get("interfaces");
      JsonNode security = server.get("security");
      JsonNode endpoints = server.get("endpoints");
      assertEquals("127.0.0.1", interfaces.get("interface").get("inet-address").get("value").asText());
      assertEquals("default", security.get("security-realms").get("security-realm").get("name").asText());
      assertEquals("hotrod", endpoints.get("hotrod-connector").get("name").asText());
      assertEquals("rest", endpoints.get("rest-connector").get("name").asText());
      assertEquals("memcachedCache", endpoints.get("memcached-connector").get("cache").asText());
   }

   @Test
   public void testThreads() {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().threads());
      String dump = restResponse.getBody();

      assertEquals(MediaType.TEXT_PLAIN, restResponse.contentType());
      assertTrue(dump.contains("state=RUNNABLE"));
   }

   @Test
   public void testInfo() throws Exception {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().info());
      String body = restResponse.getBody();
      JsonNode infoNode = mapper.readTree(body);

      assertNotNull(infoNode.get("version"));
   }

   @Test
   public void testMemory() throws Exception {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().memory());
      JsonNode infoNode = mapper.readTree(restResponse.getBody());
      JsonNode memory = infoNode.get("heap");
      assertTrue(memory.get("init").asInt() > 0);
      assertTrue(memory.get("used").asInt() > 0);
      assertTrue(memory.get("committed").asInt() > 0);
   }

   @Test
   public void testEnv() throws Exception {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().env());
      JsonNode infoNode = mapper.readTree(restResponse.getBody());
      JsonNode osVersion = infoNode.get("os.version");
      assertEquals(System.getProperty("os.version"), osVersion.asText());
   }

   @Test
   public void testCacheManagerNames() throws Exception {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.cacheManagers());
      ArrayNode cacheManagers = (ArrayNode) mapper.readTree(restResponse.getBody());
      Set<String> cmNames = StreamSupport.stream(cacheManagers.spliterator(), false).map(JsonNode::asText).collect(Collectors.toSet());

      assertEquals(cmNames, Sets.newHashSet("default"));
   }
}
