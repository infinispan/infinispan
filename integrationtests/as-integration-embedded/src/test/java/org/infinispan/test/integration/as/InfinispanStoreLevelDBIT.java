package org.infinispan.test.integration.as;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
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

import static java.io.File.separator;
import static org.junit.Assert.assertEquals;

/**
 * Test the Infinispan LevelDB CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class InfinispanStoreLevelDBIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap.create(WebArchive.class, "leveldb.war").addClass(InfinispanStoreLevelDBIT.class).add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.MODULE_SLOT + " services, org.infinispan.persistence.leveldb:" + Version.MODULE_SLOT + " services").exportAsString();
      return new StringAsset(manifest);
   }

   /**
    * To avoid pulling in TestingUtil and its plethora of dependencies
    */
   private static String tmpDirectory(Class<?> test) {
      String prefix = System.getProperty("infinispan.test.tmpdir", System.getProperty("java.io.tmpdir"));
      return prefix + separator + "infinispanTempFiles" + separator + test.getSimpleName();
   }

   @Test
   public void testCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addStore(LevelDBStoreConfigurationBuilder.class)
            .location(tmpDirectory(this.getClass()));

      EmbeddedCacheManager cm = new DefaultCacheManager(builder.build());
      Cache<String, String> cache = cm.getCache();
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
      cm.stop();
   }
}
