package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.commons.test.Eventually.eventually;
import static org.infinispan.server.functional.ShutdownRestIT.isServerShutdown;
import static org.infinispan.server.test.core.Common.assertStatus;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 10.1
 */
@Tag("embedded")
public class ConcurrentShutdownRestIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
                                    .numServers(2)
                                    .build();

   @Test
   public void testShutDown() {
      RestClient client0 = SERVER.rest().create();
      RestClient client1 = SERVER.rest().get(1);
      CompletionStage<RestResponse> stop0 = client0.server().stop();
      CompletionStage<RestResponse> stop1 = client1.server().stop();
      assertStatus(NO_CONTENT, stop0);
      assertStatus(NO_CONTENT, stop1);

      eventually(() -> isServerShutdown(client0));
      eventually(() -> isServerShutdown(client1));
      eventually(() -> !SERVER.getServerDriver().isRunning(0));
      eventually(() -> !SERVER.getServerDriver().isRunning(1));
   }
}
