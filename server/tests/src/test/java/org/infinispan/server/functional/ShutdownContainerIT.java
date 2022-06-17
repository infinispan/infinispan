package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.client.rest.RestResponse.SERVICE_UNAVAILABLE;
import static org.infinispan.server.test.core.Common.assertStatus;

import org.infinispan.client.rest.RestClient;
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
      assertStatus(OK, client.cacheManager(containerName).caches());

      assertStatus(NO_CONTENT, client.container().shutdown());

      // Ensure operations on the cachemanager are not possible
      assertStatus(SERVICE_UNAVAILABLE, client.cacheManager(containerName).caches());


      assertStatus(SERVICE_UNAVAILABLE, client.counters());

      // Ensure that the K8s liveness pods will not fail
      assertStatus(OK, client.cacheManager(containerName).healthStatus());
   }
}
