package org.infinispan.server.functional;

import static org.infinispan.commons.util.Eventually.eventually;
import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertFalse;

import java.net.ConnectException;

import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @since 10.0
 */
public class ShutdownRestIT {

   @ClassRule
   public static final InfinispanServerRule SERVER = new InfinispanServerRule(
         new InfinispanServerRuleConfigurationBuilder("configuration/ClusteredServerTest.xml").numServers(1));

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

   @Test
   public void testShutDown() {
      RestClient client = SERVER_TEST.rest().create();
      sync(client.server().stop());
      eventually(() -> isServerShutdown(client));
      assertFalse(SERVER.getServerDriver().isRunning(0));
   }

   static boolean isServerShutdown(RestClient client) {
      try {
         sync(client.server().configuration());
      } catch (RuntimeException r) {
         return (Util.getRootCause(r) instanceof ConnectException);
      }
      return false;
   }
}
