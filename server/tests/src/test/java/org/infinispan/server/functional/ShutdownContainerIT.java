package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.client.rest.RestResponse.SERVICE_UNAVAILABLE;
import static org.infinispan.server.test.core.Common.assertStatus;

import org.infinispan.client.rest.RestClient;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Ryan Emerson
 * @since 13.0
 */
public class ShutdownContainerIT {
   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .build();

   @Test
   public void testShutDown() {
      RestClient client = SERVER.rest().get();

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
