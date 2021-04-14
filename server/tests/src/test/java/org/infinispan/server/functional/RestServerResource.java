package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.Sets;

/**
 * @since 10.0
 */
public class RestServerResource {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testConfig() {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().configuration());
      assertEquals(200, restResponse.getStatus());
      Json configNode = Json.read(restResponse.getBody());

      Json server = configNode.at("server");
      Json interfaces = server.at("interfaces");
      Json security = server.at("security");
      Json endpoints = server.at("endpoints");

      String inetAddress = SERVERS.getServerDriver() instanceof ContainerInfinispanServerDriver ? "SITE_LOCAL" : "127.0.0.1";
      assertEquals(inetAddress, interfaces.at(0).at("inet-address").at("value").asString());
      assertEquals("default", security.at("security-realms").at(0).at("name").asString());
      assertEquals("hotrod", endpoints.at("hotrod-connector").at("name").asString());
      assertEquals("rest", endpoints.at("rest-connector").at("name").asString());
      assertEquals("memcachedCache", endpoints.at("memcached-connector").at("cache").asString());
   }

   @Test
   public void testThreads() {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().threads());
      String dump = restResponse.getBody();
      assertEquals(200, restResponse.getStatus());
      assertEquals(MediaType.TEXT_PLAIN, restResponse.contentType());
      assertTrue(dump.contains("state=RUNNABLE"));
   }

   @Test
   public void testInfo() {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().info());
      String body = restResponse.getBody();
      assertEquals(200, restResponse.getStatus());
      Json infoNode = Json.read(body);
      assertNotNull(infoNode.at("version"));
   }

   @Test
   public void testMemory() {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().memory());
      assertEquals(200, restResponse.getStatus());
      Json infoNode = Json.read(restResponse.getBody());
      Json memory = infoNode.at("heap");
      assertTrue(memory.at("used").asInteger() > 0);
      assertTrue(memory.at("committed").asInteger() > 0);
   }

   @Test
   public void testEnv() {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.server().env());
      assertEquals(200, restResponse.getStatus());
      Json infoNode = Json.read(restResponse.getBody());
      Json osVersion = infoNode.at("os.version");
      assertEquals(System.getProperty("os.version"), osVersion.asString());
   }

   @Test
   public void testCacheManagerNames() {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse restResponse = sync(client.cacheManagers());
      assertEquals(200, restResponse.getStatus());
      Json cacheManagers = Json.read(restResponse.getBody());
      Set<String> cmNames = cacheManagers.asJsonList().stream().map(Json::asString).collect(Collectors.toSet());

      assertEquals(cmNames, Sets.newHashSet("default"));
   }
}
