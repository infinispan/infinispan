package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.function.Function;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
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
   public void testRestRouting() throws IOException {
      Function<String, RestClientConfigurationBuilder> cfgFromCtx = c -> new RestClientConfigurationBuilder().contextPath(c);

      try (RestClient restCtx = SERVER_TEST.newRestClient(cfgFromCtx.apply("/rest"));
           RestClient invalidCtx = SERVER_TEST.newRestClient(cfgFromCtx.apply("/invalid"));
           RestClient emptyCtx = SERVER_TEST.newRestClient(cfgFromCtx.apply("/"))) {

         String body = assertStatus(OK, restCtx.server().info());
         assertTrue(body, body.contains("version"));

         assertStatus(NOT_FOUND, emptyCtx.server().info());

         assertStatus(NOT_FOUND, invalidCtx.server().info());
      }
   }
}
