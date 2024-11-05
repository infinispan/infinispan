package org.infinispan.server.resp;

import static org.infinispan.server.resp.test.RespTestingUtil.ADMIN;
import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killServer;
import static org.infinispan.server.resp.test.RespTestingUtil.startServer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.server.resp.test.TestSetup;
import org.infinispan.server.resp.test.RespAuthenticationConfigurer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.protocol.ProtocolVersion;

public abstract class AbstractRespTest extends MultipleCacheManagersTest {

   private List<RespServer> servers;
   private List<AbstractRedisClient> clients;
   private boolean authorization;

   protected int timeout = 15_000;
   protected final ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected void createCacheManagers() {
      TestSetup setup = setup();
      if (authorization)
         setup = TestSetup.authorizationEnabled(setup);

      servers = new ArrayList<>(setup.clusterSize());
      clients = new ArrayList<>(setup.clusterSize());

      for (int i = 0; i < setup.clusterSize(); i++) {
         EmbeddedCacheManager ecm = setup.createCacheManager(this::defaultRespConfiguration, this::amendGlobalConfiguration, this::amendConfiguration);

         TestingUtil.replaceComponent(ecm, TimeService.class, timeService, true);
         cacheManagers.add(ecm);

         RespServer server = createRespServer(i, ecm);
         servers.add(server);
      }

      for (RespServer server : servers) {
         RedisClient c = createRedisClient(server.getPort());
         if (c == null) continue;

         c.setOptions(defineRespClientOptions());
         clients.add(c);
      }

      afterSetupFinished();
   }

   @SuppressWarnings("unchecked")
   protected final <T extends AbstractRespTest> T self() {
      return (T) this;
   }

   protected <T extends AbstractRespTest> T withAuthorization() {
      this.authorization = true;
      return self();
   }

   protected void amendGlobalConfiguration(GlobalConfigurationBuilder builder) { }

   protected abstract TestSetup setup();

   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) { }

   protected void afterSetupFinished() { }

   protected ClientOptions defineRespClientOptions() {
      return ClientOptions.builder()
            .protocolVersion(ProtocolVersion.RESP3)
            .timeoutOptions(TimeoutOptions.enabled(Duration.of(15, ChronoUnit.SECONDS)))
            .build();
   }

   protected RedisClient createRedisClient(int port) {
      if (authorization)
         return RespAuthenticationConfigurer.createAuthenticationClient(port);

      return createClient(timeout, port);
   }

   protected RespServer createRespServer(int i, EmbeddedCacheManager ecm) {
      RespServerConfiguration serverConfiguration = serverConfiguration(i).build();
      return authorization
            ? Security.doAs(ADMIN, () -> startServer(ecm, serverConfiguration))
            : startServer(ecm, serverConfiguration);
   }

   protected RespServerConfigurationBuilder serverConfiguration(int i) {
      String serverName = TestResourceTracker.getCurrentTestShortName();
      RespServerConfigurationBuilder rscb = new RespServerConfigurationBuilder().name(serverName)
            .host(RespTestingUtil.HOST)
            .port(RespTestingUtil.port() + i);

      return authorization
            ? RespAuthenticationConfigurer.enableAuthentication(rscb)
            : rscb;
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

   protected final boolean isAuthorizationEnabled() {
      return authorization;
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      if (clients != null) {
         for (AbstractRedisClient client : clients) {
            killClient(client);
         }
      }

      if (servers != null) {
         for (RespServer server : servers) {
            killServer(server);
         }
      }

      super.destroy();
   }

   @Override
   protected String parameters() {
      return "[authz=" + authorization + "]";
   }
}
