package test.infinispan.integration.remote;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.spring.remote.provider.SpringRemoteCacheManager;
import org.infinispan.spring.remote.session.configuration.EnableInfinispanRemoteHttpSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.test.annotation.DirtiesContext;

import test.infinispan.integration.AbstractSpringSessionTCK;

@SpringBootTest(classes = RemoteSessionApp.class,
      webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
      properties = "spring.main.banner-mode=off")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RemoteSpringSessionTest extends AbstractSpringSessionTCK {

   private static EmbeddedCacheManager serverCache;

   private static HotRodServer server;

   @Autowired
   CacheManager cacheManager;

   @BeforeAll
   public static void beforeClass() {
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalConfigurationBuilder.globalState().disable();
      globalConfigurationBuilder.jmx().disable();
      ConfigurationBuilder cacheConfiguration = new ConfigurationBuilder();
      cacheConfiguration.encoding().key().mediaType(APPLICATION_SERIALIZED_OBJECT_TYPE);
      cacheConfiguration.encoding().value().mediaType(APPLICATION_SERIALIZED_OBJECT_TYPE);

      cacheConfiguration.statistics().disable();
      serverCache = new DefaultCacheManager(globalConfigurationBuilder.build());
      serverCache.defineConfiguration(EnableInfinispanRemoteHttpSession.DEFAULT_CACHE_NAME, cacheConfiguration.build());

      HotRodServerConfigurationBuilder hotRodServerConfigurationBuilder = new HotRodServerConfigurationBuilder();
      hotRodServerConfigurationBuilder.name(RemoteSpringSessionTest.class.getSimpleName());
      hotRodServerConfigurationBuilder.port(RemoteSessionApp.SERVER_PORT).defaultCacheName(EnableInfinispanRemoteHttpSession.DEFAULT_CACHE_NAME);

      server = new HotRodServer();
      server.start(hotRodServerConfigurationBuilder.build(), serverCache);
      server.postStart();
   }

   @AfterAll
   public static void afterClass() {
      server.stop();
      serverCache.stop();
   }

   @Test
   public void testCacheManagerBean() {
      assertNotNull(cacheManager);
      assertInstanceOf(SpringRemoteCacheManager.class, cacheManager);
      RemoteCacheManager nativeCacheManager = ((SpringRemoteCacheManager) cacheManager).getNativeCacheManager();
      assertNotNull(nativeCacheManager);
   }

}
