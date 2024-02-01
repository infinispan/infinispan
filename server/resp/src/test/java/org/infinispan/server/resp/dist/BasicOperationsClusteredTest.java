package org.infinispan.server.resp.dist;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.AbstractRespTest;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

@Test(groups = "functional", testName = "dist.server.resp.BasicOperationsClusteredTest")
public class BasicOperationsClusteredTest extends AbstractRespTest {

   private static final int CLUSTER_SIZE = 3;

   private RedisClusterClient client;

   private CacheMode mode;

   private BasicOperationsClusteredTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
   }

   @Test
   public void testBasicCommandsClusteredClient() {
      try (StatefulRedisClusterConnection<String, String> conn = client.connect()) {
         RedisAdvancedClusterCommands<String, String> redis = conn.sync();

         redis.set("k1", "v1");
         String v = redis.get("k1");
         assertThat(v).isEqualTo("v1");

         redis.del("k1");

         assertThat(redis.get("k1")).isNull();
         assertThat(redis.get("something")).isNull();
      }
   }

   @Test
   public void testCommandRouting() {
      try (StatefulRedisConnection<String, String> conn = this.<RedisClient>client(0).connect()) {
         RedisCommands<String, String> redis = conn.sync();

         for (int i = 1; i < CLUSTER_SIZE; i++) {
            String k = getStringKeyForCache(respCache(2));

            redis.set(k, "value");
            String v = redis.get(k);
            assertThat(v).isEqualTo("value");

            redis.del(k);

            assertThat(redis.get(k)).isNull();
         }
      }
   }

   @Override
   protected void afterSetupFinished() {
      List<RedisURI> uris = new ArrayList<>(CLUSTER_SIZE);
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         RespServer server = server(i);
         uris.add(RedisURI.Builder
               .redis(RespTestingUtil.HOST)
               .withTimeout(Duration.ofMillis(timeout))
               .withPort(server.getPort())
               .build());
      }

      client = RedisClusterClient.create(uris);
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new BasicOperationsClusteredTest().withCacheMode(CacheMode.DIST_SYNC),
            new BasicOperationsClusteredTest().withCacheMode(CacheMode.REPL_SYNC),
      };
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      configurationBuilder.clustering().cacheMode(mode);
   }

   @Override
   protected String parameters() {
      return "[mode=" + mode + "]";
   }

   @Override
   protected TestSetup setup() {
      return TestSetup.clusteredTestSetup(CLUSTER_SIZE);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      RespTestingUtil.killClient(client);
      super.destroy();
   }
}
