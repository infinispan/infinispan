package org.infinispan.test.integration.as;

import static org.junit.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan AS module integration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@RunWith(Arquillian.class)
public class InfinispanCoreIT {

   private EmbeddedCacheManager cm;

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "simple.war")
            .addClass(InfinispanCoreIT.class)
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class).attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @After
   public void cleanUp() {
      if (cm != null)
         cm.stop();
   }

   @Test
   public void testCacheManager() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.globalJmxStatistics().allowDuplicateDomains(true);

      cm = new DefaultCacheManager(gcb.build(), new ConfigurationBuilder().build());
      Cache<String, String> cache = cm.getCache();
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
   }
}
