package org.infinispan.integrationtests.spring.boot.session.remote;

import org.infinispan.commons.equivalence.AnyServerEquivalence;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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
      ConfigurationBuilder cacheConfiguration = new ConfigurationBuilder();
      cacheConfiguration.dataContainer().keyEquivalence(new AnyServerEquivalence());

      serverCache = new DefaultCacheManager();
      serverCache.defineConfiguration("sessions", cacheConfiguration.build());

      HotRodServerConfigurationBuilder hotRodServerConfigurationBuilder = new HotRodServerConfigurationBuilder();
      hotRodServerConfigurationBuilder.port(RemoteConfiguration.SERVER_PORT);

      server = new HotRodServer();
      server.start(hotRodServerConfigurationBuilder.build(), serverCache);
   }

   @AfterClass
   public static void afterClass() {
      server.stop();
      serverCache.stop();
   }

}
