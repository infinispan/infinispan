package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertResponse;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.api.Test;

/**
 * @since 10.0
 */
public class RestServerResource {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   @Test
   public void testConfig() {
      RestClient client = SERVERS.rest().create();
      Json configNode = Json.read(assertStatus(OK, client.server().configuration()));

      Json server = configNode.at("server");
      Json interfaces = server.at("interfaces");
      Json security = server.at("security");
      Json endpoints = server.at("endpoints");
      Json endpoint = endpoints.at("endpoint");

      String inetAddress = SERVERS.isContainerized() ? "0.0.0.0" : "127.0.0.1";
      assertEquals(inetAddress, interfaces.at(0).at("inet-address").at("value").asString());
      assertEquals("default", security.at("security-realms").at(0).at("name").asString());
      assertEquals("hotrod", endpoint.at("hotrod-connector").at("name").asString());
      assertEquals("rest", endpoint.at("rest-connector").at("name").asString());
      assertEquals("memcachedCache2", endpoint.at("memcached-connector").at("cache").asString());
   }

   @Test
   public void testThreads() {
      RestClient client = SERVERS.rest().create();

      assertResponse(OK, client.server().threads(), r -> {
         assertEquals(MediaType.TEXT_PLAIN, r.contentType());
         assertTrue(r.body().contains("state=RUNNABLE"));
      });
   }

   @Test
   public void testInfo() {
      RestClient client = SERVERS.rest().create();
      Json infoNode = Json.read(assertStatus(OK, client.server().info()));
      assertNotNull(infoNode.at("version"));
      if (infoNode.has("cache-manager-name")) {
         // Introduced in 16.0
         assertEquals("default", infoNode.at("cache-manager-name").asString());
      }
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
   public void testCacheDefaults() {
      RestClient client = SERVERS.rest().create();
      Json cacheDefaults = Json.read(assertStatus(OK, client.server().cacheConfigDefaults()));
      assertEquals("HEAP", cacheDefaults.at("local-cache").at("memory").at("storage").asString());
      assertEquals(2, cacheDefaults.at("local-cache").at("clustering").at("hash").at("owners").asInteger());
      assertEquals(2, cacheDefaults.at("local-cache").at("clustering").at("hash").at("owners").asInteger());
      assertEquals(-1, cacheDefaults.at("local-cache").at("expiration").at("lifespan").asInteger());
      assertEquals("REPEATABLE_READ", cacheDefaults.at("local-cache").at("locking").at("isolation").asString());
      assertEquals("30s", cacheDefaults.at("local-cache").at("transaction").at("reaper-interval").asString());
      assertEquals("30s", cacheDefaults.at("local-cache").at("sites").at("max-cleanup-delay").asString());
   }
}
