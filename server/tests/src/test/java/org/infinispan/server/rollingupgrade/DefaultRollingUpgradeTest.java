package org.infinispan.server.rollingupgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler.STATE.REMOVED_OLD;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.util.ByRef;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.junit.jupiter.api.Test;

public class DefaultRollingUpgradeTest {
   @Test
   public void testDefaultSetting() throws InterruptedException {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder("15.2.0.Final", "15.2.1.Final");
      RollingUpgradeHandler.performUpgrade(builder.build());
   }

   @Test
   public void testHttpClient() throws Exception {
      String cacheName = "rolling-upgrade";
      TestUser user = TestUser.ADMIN;
      int nodeCount = 3;
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder("15.2.0.Final", "15.2.1.Final")
            .nodeCount(nodeCount);
      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      restBuilder.security().authentication().enable().username(user.getUser()).password(user.getPassword());
      ByRef.Integer interactions = new ByRef.Integer(0);
      ByRef.Integer node = new ByRef.Integer(0);
      builder.handlers(
            uh -> {
               // First creates the cache as replicated.
               uh.getRemoteCacheManager()
                     .administration()
                     .createCache(cacheName, new StringConfiguration("<replicated-cache></replicated-cache>"));

               // Since this is the first check, we can connect to any of the nodes in the previous version.
               RestResponse res = await(uh.rest(0, restBuilder).cache(cacheName).put("foo", "bar"));
               assertThat(res.status()).isEqualTo(204);
               res.close();
               interactions.inc();
               assertThat(interactions.get()).isOne();
            },
            uh -> {
               if (uh.getCurrentState() == REMOVED_OLD)
                  return true;

               RestClient client = uh.rest(node.get(), restBuilder);
               node.inc();
               RestResponse res = await(client.cache(cacheName).get("foo"));

               assertThat(res.status()).isEqualTo(200);
               assertThat(res.body()).isEqualTo("bar");
               res.close();
               interactions.inc();
               try (RestResponse health = await(client.container().health())) {
                  return health.status() == 200;
               }
            }
      );
      RollingUpgradeHandler.performUpgrade(builder.build());

      // Initial interaction plus one for each node added.
      assertThat(interactions.get()).isEqualTo(1 + nodeCount);
   }
}
