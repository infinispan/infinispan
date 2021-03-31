package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @since 11.0
 */
public class RestLoggingResource {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testListLoggers() {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse response = sync(client.server().logging().listLoggers());
      Json loggers = Json.read(response.getBody());
      assertTrue(loggers.asJsonList().size() > 0);
   }

   @Test
   public void testListAppenders() {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse response = sync(client.server().logging().listAppenders());
      String body = response.getBody();
      Json appenders = Json.read(body);
      int expected = SERVERS.getServerDriver() instanceof ContainerInfinispanServerDriver ? 5 : 6;
      assertEquals(body, expected, appenders.asMap().size());
   }

   @Test
   public void testManipulateLogger() {
      RestClient client = SERVER_TEST.rest().create();
      // Create the logger
      RestResponse response = sync(client.server().logging().setLogger("org.infinispan.TESTLOGGER", "WARN", "STDOUT"));
      assertEquals(204, response.getStatus());
      response = sync(client.server().logging().listLoggers());
      assertTrue("Logger not found", findLogger(response, "org.infinispan.TESTLOGGER", "WARN", "STDOUT"));
      // Update it
      response = sync(client.server().logging().setLogger("org.infinispan.TESTLOGGER", "ERROR", "FILE"));
      assertEquals(204, response.getStatus());
      response = sync(client.server().logging().listLoggers());
      assertTrue("Logger not found", findLogger(response, "org.infinispan.TESTLOGGER", "ERROR", "FILE"));
      // Remove it
      response = sync(client.server().logging().removeLogger("org.infinispan.TESTLOGGER"));
      assertEquals(204, response.getStatus());
      response = sync(client.server().logging().listLoggers());
      assertFalse("Logger should not be found", findLogger(response, "org.infinispan.TESTLOGGER", "ERROR"));
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
