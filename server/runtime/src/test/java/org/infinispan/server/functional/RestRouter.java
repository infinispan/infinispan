package org.infinispan.server.functional;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.function.Function;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @since 10.0
 */
public class RestRouter {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testRestRouting() {
      Function<String, RestClientConfiguration> cfgFromCtx = c -> new RestClientConfigurationBuilder().contextPath(c).build();

      try (RestClient restCtx = RestClient.forConfiguration(cfgFromCtx.apply("/rest"));
           RestClient invalidCtx = RestClient.forConfiguration(cfgFromCtx.apply("/invalid"));
           RestClient emptyCtx = RestClient.forConfiguration(cfgFromCtx.apply("/"))) {

         RestResponse response = sync(restCtx.server().info());
         assertEquals(200, response.getStatus());
         assertTrue(response.getBody().contains("version"));

         response = sync(emptyCtx.server().info());
         assertEquals(404, response.getStatus());
         assertFalse(response.getBody().contains("cache"));

         response = sync(invalidCtx.server().info());
         assertEquals(404, response.getStatus());

      } catch (IOException ignored) {
      }
   }
}
