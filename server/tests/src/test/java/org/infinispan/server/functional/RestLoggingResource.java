package org.infinispan.server.functional;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @since 11.0
 */
public class RestLoggingResource {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final ObjectMapper mapper = new ObjectMapper();

   @Test
   public void testListLoggers() throws Exception {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse response = sync(client.server().logging().listLoggers());
      JsonNode loggers = mapper.readTree(response.getBody());
      assertTrue(loggers.size() > 0);
   }

   @Test
   public void testListAppenders() throws Exception {
      RestClient client = SERVER_TEST.rest().create();
      RestResponse response = sync(client.server().logging().listAppenders());
      String body = response.getBody();
      JsonNode appenders = mapper.readTree(body);
      int expected = SERVERS.getServerDriver() instanceof ContainerInfinispanServerDriver ? 5 : 2;
      assertEquals(body,  expected, appenders.size());
   }

   @Test
   public void testManipulateLogger() throws Exception {
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

   private boolean findLogger(RestResponse response, String name, String level, String... appenders) throws JsonProcessingException {
      JsonNode loggers = mapper.readTree(response.getBody());
      for (int i = 0; i < loggers.size(); i++) {
         JsonNode logger = loggers.get(i);
         if (name.equals(logger.get("name").asText())) {
            assertEquals(level, logger.get("level").asText());
            JsonNode loggerAppenders = logger.get("appenders");
            assertEquals(appenders.length, loggerAppenders.size());
            for (int j = 0; j < appenders.length; j++) {
               assertEquals(appenders[j], loggerAppenders.get(j).asText());
            }
            return true;
         }
      }
      return false;
   }
}
