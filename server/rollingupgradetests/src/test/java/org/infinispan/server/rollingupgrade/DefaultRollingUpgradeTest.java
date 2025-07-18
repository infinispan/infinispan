package org.infinispan.server.rollingupgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler.STATE.REMOVED_OLD;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.util.ByRef;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.Partitions;
import net.spy.memcached.ClientMode;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;

public class DefaultRollingUpgradeTest {
   @Test
   public void testDefaultSetting() throws InterruptedException {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(DefaultRollingUpgradeTest.class.getName(),
            RollingUpgradeTestUtil.getFromVersion(), RollingUpgradeTestUtil.getToVersion());
      RollingUpgradeHandler.performUpgrade(builder.build());
   }

   @Test
   public void testHttpClient() throws Exception {
      String cacheName = "rolling-upgrade";
      TestUser user = TestUser.ADMIN;
      int nodeCount = 3;
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(DefaultRollingUpgradeTest.class.getName(),
            RollingUpgradeTestUtil.getFromVersion(), RollingUpgradeTestUtil.getToVersion())
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

      // At least the initial interaction plus one for each node added.
      assertThat(interactions.get()).isGreaterThanOrEqualTo(1 + nodeCount);
   }

   @Test
   public void testRespClient() throws Exception {
      TestUser user = TestUser.ADMIN;
      int nodeCount = 3;
      ByRef.Integer interactions = new ByRef.Integer(0);
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(DefaultRollingUpgradeTest.class.getName(),
            RollingUpgradeTestUtil.getFromVersion(), RollingUpgradeTestUtil.getToVersion())
            .nodeCount(nodeCount);

      RedisURI.Builder respBuilder = RedisURI.builder()
                  .withAuthentication(user.getUser(), user.getPassword());

      builder.handlers(
            uh -> {
               RedisClusterClient client = uh.resp(respBuilder);
               Partitions partitions = client.getPartitions();
               assertThat(partitions.size()).isEqualTo(nodeCount);
               try (StatefulRedisClusterConnection<String, String> conn = client.connect()) {
                  conn.sync().set("foo", "bar");
               }
               interactions.inc();
            },
            uh -> {
               RedisClusterClient client = uh.resp(respBuilder);
               client.refreshPartitions();

               try (StatefulRedisClusterConnection<String, String> conn = client.connect()) {
                  assertThat(conn.sync().get("foo")).isEqualTo("bar");
               }

               Partitions partitions = client.getPartitions();
               interactions.inc();
               return uh.getCurrentState() == REMOVED_OLD
                     ? partitions.size() == nodeCount - 1
                     : partitions.size() == nodeCount;
            }
      );

      RollingUpgradeHandler.performUpgrade(builder.build());

      // At least the initial interaction plus one when removing and one when adding a node.
      assertThat(interactions.get()).isGreaterThanOrEqualTo(1 + 2 * nodeCount);
   }

   @Test
   public void testMemcachedClient() throws Exception {
      TestUser user = TestUser.ADMIN;
      int nodeCount = 3;
      ByRef.Integer interactions = new ByRef.Integer(0);
      ByRef.Integer node = new ByRef.Integer(0);
      ConnectionFactoryBuilder memcachedBuilder = new ConnectionFactoryBuilder()
            .setClientMode(ClientMode.Static)
            .setOpTimeout(TimeUnit.SECONDS.toMillis(30))
            .setAuthDescriptor(AuthDescriptor.typical(user.getUser(), user.getPassword()));

      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(DefaultRollingUpgradeTest.class.getName(),
            RollingUpgradeTestUtil.getFromVersion(), RollingUpgradeTestUtil.getToVersion())
            .nodeCount(nodeCount);
      builder.handlers(
            uh -> {
               MemcachedClient client = uh.memcached(0, memcachedBuilder);
               join(client.set("foo", 0, "bar"));
               interactions.inc();
            },
            uh -> {
               if (uh.getCurrentState() == REMOVED_OLD)
                  return true;

               MemcachedClient client = uh.memcached(node.get(), memcachedBuilder);
               node.inc();

               try {
                  return "bar".equals(client.get("foo"));
               } catch (Throwable ignore) {
                  return false;
               } finally {
                  interactions.inc();
               }
            }
      );

      RollingUpgradeHandler.performUpgrade(builder.build());

      // At least the initial interaction plus one for each node added.
      assertThat(interactions.get()).isGreaterThanOrEqualTo(1 + nodeCount);
   }

   private static void join(Future<?> future) {
      try {
         future.get(10, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
         throw new AssertionError(e);
      }
   }
}
