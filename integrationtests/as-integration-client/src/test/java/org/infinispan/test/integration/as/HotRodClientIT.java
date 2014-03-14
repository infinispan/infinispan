package org.infinispan.test.integration.as;

import org.infinispan.Version;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Test the Infinispan AS remote client module integration
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class HotRodClientIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "remote-client.war")
            .addClass(HotRodClientIT.class)
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   @Test
   public void testCacheManager() {
      RemoteCacheManager rcm = createCacheManager();
      RemoteCache<String, String> cache = rcm.getCache();
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
      rcm.stop();
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class).attribute("Dependencies", "org.infinispan.client.hotrod:" + Version.MODULE_SLOT + " services").exportAsString();
      return new StringAsset(manifest);
   }

   private static RemoteCacheManager createCacheManager() {
      return new RemoteCacheManager(createConfiguration(), true);
   }

   private static Configuration createConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer().host("127.0.0.1");
      return config.build();
   }

}
