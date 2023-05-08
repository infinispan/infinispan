package org.infinispan.server.resp;

import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killServer;

import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.CRC16HashFunctionPartitioner;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.test.MultipleCacheManagersTest;
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
   protected static final int timeout = 60;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      amendCacheConfiguration(cacheBuilder);
      cacheBuilder.clustering().hash().keyPartitioner(new CRC16HashFunctionPartitioner()).numSegments(256);
      createCluster(cacheBuilder, 2);
      waitForClusterToForm();

      server1 = RespTestingUtil.startServer(cacheManagers.get(0), serverConfiguration(0).build());
      server2 = RespTestingUtil.startServer(cacheManagers.get(1), serverConfiguration(1).build());
      client1 = createClient(30000, server1.getPort());
      client2 = createClient(30000, server2.getPort());
      redisConnection1 = client1.connect();
      redisConnection2 = client2.connect();
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
