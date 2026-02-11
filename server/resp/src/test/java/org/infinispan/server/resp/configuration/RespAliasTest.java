package org.infinispan.server.resp.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Testing.tmpDirectory;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.TestResourceTracker;
import org.testng.annotations.Test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "resp.configuration.RespAliasTest")
public class RespAliasTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() {
      String stateDir = tmpDirectory(this.getClass().getSimpleName());
      Util.recursiveFileRemove(stateDir);

      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.globalState().enable()
            .persistentLocation(stateDir)
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .sharedPersistentLocation(stateDir);

      EmbeddedCacheManager ecm = TestCacheManagerFactory.createClusteredCacheManager(gcb, null);

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.aliases("0");
      ecm.defineConfiguration("other-cache", cb.build());

      return ecm;
   }

   public void testRespCache() {
      RespServer server = RespTestingUtil.startServer(cacheManager);

      assertSimpleOperations(server);
      RespTestingUtil.killServer(server);
   }

   public void testConnectorUseAlias() {
      String serverName = TestResourceTracker.getCurrentTestShortName();
      String alias = "alias-" + serverName;

      ConfigurationBuilder cb = RespTestingUtil.defaultRespConfiguration();
      cb.aliases(alias);
      cacheManager.defineConfiguration("cache-" + serverName, cb.build());

      RespServerConfigurationBuilder rscb = new RespServerConfigurationBuilder().name(serverName)
            .defaultCacheName(alias)
            .host(RespTestingUtil.HOST).port(RespTestingUtil.port());
      RespServer server = RespTestingUtil.startServer(cacheManager, rscb.build());

      assertSimpleOperations(server);
      RespTestingUtil.killServer(server);
   }

   private static void assertSimpleOperations(RespServer server) {
      RedisClient c = RespTestingUtil.createClient(10_000, server.getPort());
      try (StatefulRedisConnection<String, String> conn = c.connect()) {
         RedisCommands<String, String> redis = conn.sync();

         redis.set("key", "value");
         assertThat(redis.get("key")).isEqualTo("value");
      }

      RespTestingUtil.killClient(c);
   }
}
