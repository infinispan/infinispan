package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.function.Function;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 10.0
 */
public class RestRouter {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @Test
   public void testRestRouting() throws IOException {
      Function<String, RestClient> client = c -> SERVERS.rest()
            .withClientConfiguration(new RestClientConfigurationBuilder().contextPath(c))
            .get();

      try (RestClient restCtx = client.apply("/rest");
           RestClient invalidCtx = client.apply("/invalid");
           RestClient emptyCtx = client.apply("/")) {

         String body = assertStatus(OK, restCtx.server().info());
         assertTrue(body.contains("version"), body);

         assertStatus(NOT_FOUND, emptyCtx.server().info());

         assertStatus(NOT_FOUND, invalidCtx.server().info());
      }
   }
}
