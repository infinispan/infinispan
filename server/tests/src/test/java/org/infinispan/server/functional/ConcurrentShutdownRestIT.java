package org.infinispan.server.functional;

import static org.infinispan.commons.test.Eventually.eventually;
import static org.infinispan.server.functional.ShutdownRestIT.isServerShutdown;
import static org.infinispan.server.test.core.Common.sync;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @since 10.1
 */
public class ConcurrentShutdownRestIT {

   @ClassRule
   public static final InfinispanServerRule SERVER =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
                                    .numServers(2)
                                    .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

   @Test
   public void testShutDown() {
      RestClient client0 = SERVER_TEST.rest().create();
      RestClient client1 = SERVER_TEST.rest().get(1);
      CompletionStage<RestResponse> stop0 = client0.server().stop();
      CompletionStage<RestResponse> stop1 = client1.server().stop();
      sync(stop0);
      sync(stop1);

      eventually(() -> isServerShutdown(client0));
      eventually(() -> isServerShutdown(client1));
      eventually(() -> !SERVER.getServerDriver().isRunning(0));
      eventually(() -> !SERVER.getServerDriver().isRunning(1));
   }
}
