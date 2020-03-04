package org.infinispan.integrationtests.spring.boot.session.remote;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.integrationtests.spring.boot.session.AbstractSpringSessionTCK;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.spring.remote.session.configuration.EnableInfinispanRemoteHttpSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = RemoteConfiguration.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RemoteSpringSessionTest extends AbstractSpringSessionTCK {

   private static EmbeddedCacheManager serverCache;

   private static HotRodServer server;

   @BeforeClass
   public static void beforeClass() {
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      ConfigurationBuilder cacheConfiguration = new ConfigurationBuilder();
      cacheConfiguration.encoding().key().mediaType(APPLICATION_SERIALIZED_OBJECT_TYPE);
      cacheConfiguration.encoding().value().mediaType(APPLICATION_SERIALIZED_OBJECT_TYPE);
      serverCache = new DefaultCacheManager(globalConfigurationBuilder.build());
      serverCache.defineConfiguration(EnableInfinispanRemoteHttpSession.DEFAULT_CACHE_NAME, cacheConfiguration.build());

      HotRodServerConfigurationBuilder hotRodServerConfigurationBuilder = new HotRodServerConfigurationBuilder();
      hotRodServerConfigurationBuilder.name(RemoteSpringSessionTest.class.getSimpleName());
      hotRodServerConfigurationBuilder.port(RemoteConfiguration.SERVER_PORT).defaultCacheName(EnableInfinispanRemoteHttpSession.DEFAULT_CACHE_NAME);

      server = new HotRodServer();
      server.start(hotRodServerConfigurationBuilder.build(), serverCache);
   }

   @AfterClass
   public static void afterClass() {
      server.stop();
      serverCache.stop();
   }
}
