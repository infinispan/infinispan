package org.infinispan.server.functional;

import static org.infinispan.commons.test.Eventually.eventually;
import static org.infinispan.server.test.core.Common.sync;

import java.net.ConnectException;

import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @since 10.0
 */
public class ShutdownRestIT {

   @ClassRule
   public static final InfinispanServerRule SERVER =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
                                    .numServers(1)
                                    .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

   @Test
   public void testShutDown() {
      RestClient client = SERVER_TEST.rest().create();
      sync(client.server().stop());
      eventually(() -> isServerShutdown(client));
      eventually(() -> !SERVER.getServerDriver().isRunning(0));
   }

   static boolean isServerShutdown(RestClient client) {
      try {
         sync(client.server().configuration()).close();
      } catch (RuntimeException r) {
         return (Util.getRootCause(r) instanceof ConnectException);
      }
      return false;
   }
}
