package org.infinispan.test.integration.as;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;

import static org.junit.Assert.assertEquals;

/**
 * Test the Infinispan JDBC CacheStore AS module integration.
 * N.B. the Module of the JDBC CacheStore is intentionally
 * NOT available to the deployment: it needs to be able to
 * initialize it via the dependency from the org.infinispan
 * main module.
 *
 * @author Tristan Tarrant
 * @author Sanne Grinovero
 * @since 9.0
 */
@RunWith(Arquillian.class)
public class InfinispanModulesStoreJdbcIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap.create(WebArchive.class, "jdbc.war")
            .addClass(InfinispanModulesStoreJdbcIT.class)
            .addAsResource("jdbc-config.xml")
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @Test
   public void testXmlConfig() throws IOException {
      EmbeddedCacheManager cm = null;
      try {
         cm = new DefaultCacheManager("jdbc-config.xml");
         Cache<String, String> cache = cm.getCache("anotherCache");
         cache.put("a", "a");
         assertEquals("a", cache.get("a"));
      } finally {
         if (cm != null)
            cm.stop();
      }
   }

}
