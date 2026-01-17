package org.infinispan.server.resp;

import static org.infinispan.server.resp.configuration.RespServerConfiguration.DEFAULT_RESP_CACHE;
import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killServer;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.io.File;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.testing.TestResourceTracker;
import org.testng.annotations.AfterClass;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

public abstract class BaseMultipleRespTest extends MultipleCacheManagersTest {

   protected RedisClient client1;
   protected RedisClient client2;
   protected RespServer server1;
   protected RespServer server2;
   protected StatefulRedisConnection<String, String> redisConnection1;
   protected StatefulRedisConnection<String, String> redisConnection2;
   public static final int TIMEOUT = 60;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      amendCacheConfiguration(cacheBuilder);
      cacheBuilder
            .encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM)
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().hash().keyPartitioner(new RESPHashFunctionPartitioner()).numSegments(256);

      for (int i = 0; i < 2; i++) {
         String stateDirectory = tmpDirectory(this.getClass().getSimpleName() + File.separator +  i);
         Util.recursiveFileRemove(stateDirectory);
         GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
         gcb.globalState().enable()
               .persistentLocation(stateDirectory)
               .configurationStorage(ConfigurationStorage.OVERLAY)
               .sharedPersistentLocation(stateDirectory);
         addClusterEnabledCacheManager(gcb, cacheBuilder);
      }

      defineRespConfiguration(cacheBuilder.build());
      waitForClusterToForm();

      server1 = RespTestingUtil.startServer(cacheManagers.get(0), serverConfiguration(0).build());
      server2 = RespTestingUtil.startServer(cacheManagers.get(1), serverConfiguration(1).build());
      client1 = createClient(30000, server1.getPort());
      client2 = createClient(30000, server2.getPort());
      redisConnection1 = client1.connect();
      redisConnection2 = client2.connect();
   }

   protected void defineRespConfiguration(Configuration configuration) {
      for (EmbeddedCacheManager ecm : managers()) {
         ecm.defineConfiguration(DEFAULT_RESP_CACHE, configuration);
      }
   }

   protected Cache<String, String> respCache(int index) {
      return manager(index).getCache(DEFAULT_RESP_CACHE);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      log.debug("Test finished, close resp server");
      killClient(client1);
      killClient(client2);
      killServer(server1);
      killServer(server2);
      super.destroy();
   }

   protected RespServerConfigurationBuilder serverConfiguration(int offset) {
      String serverName = TestResourceTracker.getCurrentTestShortName();
      return new RespServerConfigurationBuilder().name(serverName)
            .host(RespTestingUtil.HOST)
            .port(RespTestingUtil.port() + offset);
   }

   protected void amendCacheConfiguration(ConfigurationBuilder builder) {
   }
}
