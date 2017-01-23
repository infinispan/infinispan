package org.infinispan.integrationtests.spring.boot.session.remote;

import java.util.UUID;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.integrationtests.spring.boot.session.AbstractSpringSessionTCK;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = RemoteConfiguration.class, webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT)
public class RemoteSpringSessionTest extends AbstractSpringSessionTCK {

   private static EmbeddedCacheManager serverCache;

   private static HotRodServer server;

   @BeforeClass
   public static void beforeclass() {
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalConfigurationBuilder.globalJmxStatistics().jmxDomain("infinispan-" + UUID.randomUUID());

      ConfigurationBuilder cacheConfiguration = new ConfigurationBuilder();

      serverCache = new DefaultCacheManager(globalConfigurationBuilder.build());
      serverCache.defineConfiguration("sessions", cacheConfiguration.build());

      HotRodServerConfigurationBuilder hotRodServerConfigurationBuilder = new HotRodServerConfigurationBuilder();
      hotRodServerConfigurationBuilder.port(RemoteConfiguration.SERVER_PORT).defaultCacheName("sessions");

      server = new HotRodServer();
      server.start(hotRodServerConfigurationBuilder.build(), serverCache);
   }

   @AfterClass
   public static void afterClass() {
      server.stop();
      serverCache.stop();
   }

}
