package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.commons.test.Eventually.eventually;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;

import java.net.ConnectException;

import org.infinispan.client.rest.RestClient;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 10.0
 */
public class ShutdownRestIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .build();

   @Test
   public void testShutDown() {
      RestClient client = SERVER.rest().create();
      assertStatus(NO_CONTENT, client.server().stop());
      eventually(() -> isServerShutdown(client));
      eventually(() -> !SERVER.getServerDriver().isRunning(0));
   }

   static boolean isServerShutdown(RestClient client) {
      try {
         sync(client.server().configuration()).close();
      } catch (RuntimeException r) {
         return (r.getCause() instanceof ConnectException);
      }
      return false;
   }
}
