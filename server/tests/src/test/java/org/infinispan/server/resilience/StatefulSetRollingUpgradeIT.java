package org.infinispan.server.resilience;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Start cluster (A,B) redeploy after upgrade. Rolling upgrades always occur in the order B,A and A does not restart
 * until B has completed successfully.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class StatefulSetRollingUpgradeIT {

   private static final int NUM_NODES = 2;
   private static final int NUM_ROLLING_UPGRADES = 4;
   private static final String CACHE_MANAGER = "default";

   @ClassRule
   public static final InfinispanServerRule SERVER =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(NUM_NODES)
               .runMode(ServerRunMode.CONTAINER)
               .parallelStartup(false)
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

   @Test
   public void testStatefulSetRollingUpgrade() {
      IntStream.range(0, NUM_NODES).forEach(this::assertLiveness);
      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVER.getServerDriver();

      for (int i  = 0; i < NUM_ROLLING_UPGRADES; i++) {
         for (int j = NUM_NODES - 1; j > -1; j--) {
            serverDriver.sigterm(j);
            serverDriver.restart(j);
            assertLiveness(j);
         }
      }
   }

   private void assertLiveness(int server) {
      RestClient rest = SERVER_TEST.rest().get(server);
      RestResponse rsp = sync(rest.cacheManager(CACHE_MANAGER).healthStatus(), 10,  TimeUnit.SECONDS);
      assertEquals(HttpResponseStatus.OK.code(), rsp.getStatus());
   }
}
