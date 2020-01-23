package org.infinispan.server.functional;

import static org.infinispan.rest.helper.RestResponses.assertStatus;
import static org.infinispan.rest.helper.RestResponses.responseBody;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.function.Function;

import org.infinispan.client.rest.RestClient;
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

         String body = responseBody(restCtx.server().info());
         assertTrue(body.contains("version"));

         assertStatus(404, emptyCtx.server().info());

         assertStatus(404, invalidCtx.server().info());
      } catch (IOException ignored) {
      }
   }
}
