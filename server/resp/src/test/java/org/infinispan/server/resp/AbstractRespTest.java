package org.infinispan.server.resp;

import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killServer;
import static org.infinispan.server.resp.test.RespTestingUtil.startServer;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.server.resp.test.TestSetup;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;

import io.lettuce.core.AbstractRedisClient;

public abstract class AbstractRespTest extends MultipleCacheManagersTest {

   private List<RespServer> servers;
   private List<AbstractRedisClient> clients;

   protected int timeout = 15_000;
   protected final TimeService timeService = new ControlledTimeService();

   @Override
   protected void createCacheManagers() {
      TestSetup setup = setup();

      servers = new ArrayList<>(setup.clusterSize());
      clients = new ArrayList<>(setup.clusterSize());

      for (int i = 0; i < setup.clusterSize(); i++) {
         EmbeddedCacheManager ecm = setup.createCacheManager(this::defaultRespConfiguration, this::amendConfiguration);

         TestingUtil.replaceComponent(ecm, TimeService.class, timeService, true);
         cacheManagers.add(ecm);

         RespServer server = createRespServer(i, ecm);
         servers.add(server);
      }

      for (RespServer server : servers) {
         AbstractRedisClient c = createRedisClient(server.getPort());
         if (c == null) continue;

         clients.add(c);
      }

      afterSetupFinished();
   }

   protected abstract TestSetup setup();

   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) { }

   protected void afterSetupFinished() { }

   protected AbstractRedisClient createRedisClient(int port) {
      return createClient(timeout, port);
   }

   protected RespServer createRespServer(int i, EmbeddedCacheManager ecm) {
      RespServerConfiguration serverConfiguration = serverConfiguration(i).build();
      return startServer(ecm, serverConfiguration);
   }

   protected RespServerConfigurationBuilder serverConfiguration(int i) {
      String serverName = TestResourceTracker.getCurrentTestShortName();
      return new RespServerConfigurationBuilder().name(serverName)
            .host(RespTestingUtil.HOST)
            .port(RespTestingUtil.port() + i);
   }

   protected final <T extends AbstractRedisClient> T client(int i) {
      // noinspection unchecked
      return (T) clients.get(i);
   }

   protected final RespServer server(int i) {
      return servers.get(i);
   }

   protected final Cache<String, String> respCache(int i) {
      return manager(i).getCache(server(i).getConfiguration().defaultCacheName());
   }

   protected final ConfigurationBuilder defaultRespConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM);
      builder.clustering().hash().keyPartitioner(new RESPHashFunctionPartitioner()).numSegments(256);
      return builder;
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      for (AbstractRedisClient client : clients) {
         killClient(client);
      }

      for (RespServer server : servers) {
         killServer(server);
      }

      super.destroy();
   }
}
