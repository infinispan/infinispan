package org.infinispan.test.integration.as;

import static org.junit.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
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
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan JPA CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class InfinispanStoreJpaIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "jpa.war")
            .addClass(InfinispanStoreJpaIT.class)
            .addClass(KeyValueEntity.class)
            .addAsResource("META-INF/persistence.xml")
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.MODULE_SLOT + " services, org.infinispan.persistence.jpa:" + Version.MODULE_SLOT + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @Test
   public void testCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addStore(JpaStoreConfigurationBuilder.class)
         .persistenceUnitName("org.infinispan.persistence.jpa")
         .entityClass(KeyValueEntity.class);

      EmbeddedCacheManager cm = new DefaultCacheManager(builder.build());
      Cache<String, KeyValueEntity> cache = cm.getCache();
      KeyValueEntity entity = new KeyValueEntity("a", "a");
      cache.put(entity.getK(), entity);
      assertEquals("a", cache.get(entity.getK()).getValue());
      cm.stop();
   }

}
