package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.BAD_REQUEST;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.jupiter.InfinispanServer;
import org.junit.jupiter.api.Test;

/**
 * Tests for runtime global configuration attribute updates via REST.
 *
 * @since 16.2
 */
public class RestGlobalConfigurationTest {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   @Test
   public void testGetMutableAttributes() {
      RestClient client = SERVERS.rest().get();
      String body = assertStatus(OK, client.container().globalConfigurationAttributes(false));
      Json names = Json.read(body);
      assertTrue(names.asList().contains("metrics.accurate-size"));
   }

   @Test
   public void testGetMutableAttributesFull() {
      RestClient client = SERVERS.rest().get();
      String body = assertStatus(OK, client.container().globalConfigurationAttributes(true));
      Json attributes = Json.read(body);
      assertTrue(attributes.asJsonMap().containsKey("metrics.accurate-size"));
      assertEquals("boolean", attributes.at("metrics.accurate-size").at("type").asString());
   }

   @Test
   public void testUpdateAccurateSize() {
      RestClient client = SERVERS.rest().get();

      assertStatus(OK, client.container().updateGlobalConfigurationAttribute("metrics.accurate-size", "true"));

      String body = assertStatus(OK, client.container().globalConfigurationAttributes(true));
      Json attributes = Json.read(body);
      assertTrue(attributes.at("metrics.accurate-size").at("value").asBoolean());

      assertStatus(OK, client.container().updateGlobalConfigurationAttribute("metrics.accurate-size", "false"));

      body = assertStatus(OK, client.container().globalConfigurationAttributes(true));
      attributes = Json.read(body);
      assertFalse(attributes.at("metrics.accurate-size").at("value").asBoolean());
   }

   @Test
   public void testUpdateImmutableAttributeFails() {
      RestClient client = SERVERS.rest().get();
      assertStatus(BAD_REQUEST, client.container().updateGlobalConfigurationAttribute("metrics.gauges", "false"));
   }

   @Test
   public void testUpdatePropagatesAcrossCluster() {
      RestClient client0 = SERVERS.rest().get(0);
      RestClient client1 = SERVERS.rest().get(1);

      assertStatus(OK, client0.container().updateGlobalConfigurationAttribute("metrics.accurate-size", "true"));

      String body = assertStatus(OK, client1.container().globalConfigurationAttributes(true));
      Json attributes = Json.read(body);
      assertTrue(attributes.at("metrics.accurate-size").at("value").asBoolean());

      // Reset
      assertStatus(OK, client0.container().updateGlobalConfigurationAttribute("metrics.accurate-size", "false"));
   }
}
