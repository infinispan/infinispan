package org.infinispan.server.functional;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.Assert.assertEquals;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Ryan Emerson
 * @since 13.0
 */
public class ShutdownContainerIT {
   @ClassRule
   public static final InfinispanServerRule SERVER =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

   @Test
   public void testShutDown() {
      RestClient client = SERVER_TEST.rest().get();

      String containerName = "default";
      RestResponse response = join(client.cacheManager(containerName).caches());
      assertEquals(200, response.getStatus());

      response = join(client.container().shutdown());
      assertEquals(204, response.getStatus());

      // Ensure operations on the cachemanager are not possible
      response = join(client.cacheManager(containerName).caches());
      assertEquals(503, response.getStatus());

      response = join(client.counters());
      assertEquals(503, response.getStatus());

      // Ensure that the K8s liveness pods will not fail
      response = join(client.cacheManager(containerName).healthStatus());
      assertEquals(200, response.getStatus());
   }
}
