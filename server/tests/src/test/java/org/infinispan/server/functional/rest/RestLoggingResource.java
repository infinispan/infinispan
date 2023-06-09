package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 11.0
 */
public class RestLoggingResource {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @Test
   public void testListLoggers() {
      RestClient client = SERVERS.rest().create();
      Json loggers = Json.read(assertStatus(OK, client.server().logging().listLoggers()));
      assertTrue(loggers.asJsonList().size() > 0);
   }

   @Test
   public void testListAppenders() {
      RestClient client = SERVERS.rest().create();
      String body = assertStatus(OK, client.server().logging().listAppenders());
      Json appenders = Json.read(body);
      assertEquals(5, appenders.asMap().size(), body);
   }

   @Test
   public void testManipulateLogger() {
      RestClient client = SERVERS.rest().create();
      // Create the logger
      assertStatus(204, client.server().logging().setLogger("org.infinispan.TESTLOGGER", "WARN", "STDOUT"));
      try (RestResponse response = sync(client.server().logging().listLoggers())) {
         assertTrue(findLogger(response, "org.infinispan.TESTLOGGER", "WARN", "STDOUT"), "Logger not found");
      }

      // Update it
      assertStatus(204, client.server().logging().setLogger("org.infinispan.TESTLOGGER", "ERROR", "FILE"));
      try (RestResponse response = sync(client.server().logging().listLoggers())) {
         assertTrue(findLogger(response, "org.infinispan.TESTLOGGER", "ERROR", "FILE"), "Logger not found");
      }

      // Remove it
      assertStatus(204, client.server().logging().removeLogger("org.infinispan.TESTLOGGER"));
      try (RestResponse response = sync(client.server().logging().listLoggers())) {
         assertFalse(findLogger(response, "org.infinispan.TESTLOGGER", "ERROR"), "Logger should not be found");
      }
   }

   private boolean findLogger(RestResponse response, String name, String level, String... appenders) {
      Json loggers = Json.read(response.getBody());
      for (int i = 0; i < loggers.asJsonList().size(); i++) {
         Json logger = loggers.at(i);
         if (name.equals(logger.at("name").asString())) {
            assertEquals(level, logger.at("level").asString());
            List<Json> loggerAppenders = logger.at("appenders").asJsonList();
            assertEquals(appenders.length, loggerAppenders.size());
            for (int j = 0; j < appenders.length; j++) {
               assertEquals(appenders[j], loggerAppenders.get(j).asString());
            }
            return true;
         }
      }
      return false;
   }
}
