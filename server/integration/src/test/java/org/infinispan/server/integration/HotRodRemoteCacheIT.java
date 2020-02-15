package org.infinispan.server.integration;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for the HotRod client RemoteCache class deployed on another server
 */
@RunWith(Arquillian.class)
public class HotRodRemoteCacheIT {

   private RemoteCacheManager rcm;

   @Test
   public void testCacheManager() {
      rcm = createCacheManager();
      RemoteCache<String, String> cache = rcm.getCache("integration.REPL_SYNC");
      cache.clear();
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
      if (rcm != null)
         rcm.stop();
   }

   private static RemoteCacheManager createCacheManager() {
      return new RemoteCacheManager(createConfiguration(), true);
   }

   private static Configuration createConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer().host("127.0.0.1");
      return config.build();
   }

   @Deployment
   public static WebArchive createDeployment() {
      return DeploymentBuilder.war();
   }
}
