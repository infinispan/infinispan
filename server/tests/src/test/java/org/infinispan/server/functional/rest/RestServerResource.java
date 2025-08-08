package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertResponse;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.shaded.com.google.common.collect.Sets;

/**
 * @since 10.0
 */
public class RestServerResource {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @Test
   public void testConfig() {
      RestClient client = SERVERS.rest().create();
      Json configNode = Json.read(assertStatus(OK, client.server().configuration()));

      Json server = configNode.at("server");
      Json interfaces = server.at("interfaces");
      Json security = server.at("security");
      Json endpoints = server.at("endpoints");
      Json endpoint = endpoints.at("endpoint");

      String inetAddress = SERVERS.getServerDriver() instanceof ContainerInfinispanServerDriver ? "SITE_LOCAL"
            : "127.0.0.1";
      assertEquals(inetAddress, interfaces.at(0).at("inet-address").at("value").asString());
      assertEquals("default", security.at("security-realms").at(0).at("name").asString());
      assertEquals("hotrod", endpoint.at("hotrod-connector").at("name").asString());
      assertEquals("rest", endpoint.at("rest-connector").at("name").asString());
      if (Boolean.getBoolean(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_NEWER_THAN_14)) {
         assertEquals("memcachedCache2", endpoint.at("memcached-connector").at("cache").asString());
      } else {
         assertEquals("memcachedCache", endpoint.at("memcached-connector").at("cache").asString());
      }
   }

   @Test
   public void testThreads() {
      RestClient client = SERVERS.rest().create();

      assertResponse(OK, client.server().threads(), r -> {
         assertEquals(MediaType.TEXT_PLAIN, r.contentType());
         assertTrue(r.getBody().contains("state=RUNNABLE"));
      });
   }

   @Test
   public void testInfo() {
      RestClient client = SERVERS.rest().create();
      Json infoNode = Json.read(assertStatus(OK, client.server().info()));
      assertNotNull(infoNode.at("version"));
   }

   @Test
   public void testMemory() {
      RestClient client = SERVERS.rest().create();
      Json infoNode = Json.read(assertStatus(OK, client.server().memory()));
      Json memory = infoNode.at("heap");
      assertTrue(memory.at("used").asInteger() > 0);
      assertTrue(memory.at("committed").asInteger() > 0);
   }

   @Test
   public void testEnv() {
      RestClient client = SERVERS.rest().create();
      Json infoNode = Json.read(assertStatus(OK, client.server().env()));
      Json osVersion = infoNode.at("os.version");
      assertEquals(System.getProperty("os.version"), osVersion.asString());
   }

   @Test
   public void testCacheManagerNames() {
      RestClient client = SERVERS.rest().create();
      Json cacheManagers = Json.read(assertStatus(OK, client.cacheManagers()));
      Set<String> cmNames = cacheManagers.asJsonList().stream().map(Json::asString).collect(Collectors.toSet());
      assertEquals(cmNames, Sets.newHashSet("default"));
   }
}
