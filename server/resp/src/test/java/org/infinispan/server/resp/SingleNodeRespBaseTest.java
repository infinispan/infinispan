package org.infinispan.server.resp;

import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killServer;
import static org.infinispan.server.resp.test.RespTestingUtil.startServer;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.impl.CRC16HashFunctionPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.AfterClass;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

public abstract class SingleNodeRespBaseTest extends SingleCacheManagerTest {
   protected RedisClient client;
   protected RespServer server;
   protected StatefulRedisConnection<String, String> redisConnection;
   protected static final int timeout = 60;
   protected final TimeService timeService = new ControlledTimeService();

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      cacheManager = createTestCacheManager();
      RespServerConfiguration serverConfiguration = serverConfiguration().build();
      server = startServer(cacheManager, serverConfiguration);
      client = createClient(30000, server.getPort());
      redisConnection = client.connect();
      cache = cacheManager.getCache(server.getConfiguration().defaultCacheName());
      return cacheManager;
   }

   protected EmbeddedCacheManager createTestCacheManager() {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      TestCacheManagerFactory.amendGlobalConfiguration(globalBuilder, new TransportFlags());
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM);
      builder.clustering().hash().keyPartitioner(new CRC16HashFunctionPartitioner()).numSegments(256);
      amendConfiguration(builder);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.newDefaultCacheManager(true, globalBuilder, builder);
      TestingUtil.replaceComponent(cacheManager, TimeService.class, timeService, true);
      return cacheManager;
   }

   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {

   }

   protected RespServerConfigurationBuilder serverConfiguration() {
      String serverName = TestResourceTracker.getCurrentTestShortName();
      return new RespServerConfigurationBuilder().name(serverName)
            .host(RespTestingUtil.HOST)
            .port(RespTestingUtil.port());
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      log.debug("Test finished, close resp server");
      killClient(client);
      killServer(server);
   }
}
