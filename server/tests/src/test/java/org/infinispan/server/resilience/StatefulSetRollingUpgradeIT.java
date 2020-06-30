package org.infinispan.server.resilience;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Start cluster (0..numServers) redeploy after upgrade. Rolling upgrades always occur in the order numServers...0 and
 * the node with index numServers - 1 does not restart until node numServers has completed successfully.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@RunWith(Parameterized.class)
public class StatefulSetRollingUpgradeIT {

   private static final int NUM_ROLLING_UPGRADES = 4;
   private static final String CACHE_MANAGER = "default";

   private final int numServers;
   private InfinispanServerRule serverRule;
   private InfinispanServerTestMethodRule methodRule;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{{2}, {3}, {4}, {5}});
   }

   public StatefulSetRollingUpgradeIT(int numServers) {
      this.numServers = numServers;
   }

   @Rule
   public RuleChain getRuleChain() {
      serverRule = InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
            .numServers(numServers)
            .runMode(ServerRunMode.CONTAINER)
            .parallelStartup(false)
            .build();

      methodRule = new InfinispanServerTestMethodRule(serverRule);

      return RuleChain.outerRule(serverRule)
            .around(methodRule);
   }

   @Test
   public void testStatefulSetRollingUpgrade() {
      IntStream.range(0, numServers).forEach(this::assertLiveness);
      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) serverRule.getServerDriver();

      for (int i = 0; i < NUM_ROLLING_UPGRADES; i++) {
         for (int j = numServers - 1; j > -1; j--) {
            serverDriver.stop(j);
            serverDriver.restart(j);
            assertLiveness(j);
         }
      }
   }

   private void assertLiveness(int server) {
      RestClient rest = methodRule.rest().get(server);
      RestResponse rsp = sync(rest.cacheManager(CACHE_MANAGER).healthStatus(), 10, TimeUnit.SECONDS);
      assertEquals(HttpResponseStatus.OK.code(), rsp.getStatus());
   }
}
