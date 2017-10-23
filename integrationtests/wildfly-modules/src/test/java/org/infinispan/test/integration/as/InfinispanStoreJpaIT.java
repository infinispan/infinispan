package org.infinispan.test.integration.as;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
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
 * Test the Infinispan JPA CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class InfinispanStoreJpaIT {

   private EmbeddedCacheManager cm;

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "jpa.war")
            .addClass(InfinispanStoreJpaIT.class)
            .addClass(KeyValueEntity.class)
            .addAsResource("META-INF/persistence.xml")
            .addAsResource("jpa-config.xml")
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   @After
   public void cleanUp() {
      if (cm != null)
         cm.stop();
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services, org.infinispan.persistence.jpa:" + Version.getModuleSlot() + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @Test
   public void testCacheManager() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addStore(JpaStoreConfigurationBuilder.class)
            .persistenceUnitName("org.infinispan.persistence.jpa")
            .entityClass(KeyValueEntity.class);

      cm = new DefaultCacheManager(gcb.build(), builder.build());
      Cache<String, KeyValueEntity> cache = cm.getCache();
      KeyValueEntity entity = new KeyValueEntity("a", "a");
      cache.put(entity.getK(), entity);
      assertEquals("a", cache.get(entity.getK()).getValue());
   }

   @Test
   public void testXmlConfig() throws IOException {
      cm = new DefaultCacheManager("jpa-config.xml");
      Cache<String, KeyValueEntity> specificCache = cm.getCache("specificCache");
      validateConfig(specificCache);
      KeyValueEntity entity = new KeyValueEntity("k", "v");
      specificCache.put(entity.getK(), entity);
   }

   private void validateConfig(Cache<String, KeyValueEntity> vehicleCache) {
      StoreConfiguration config = vehicleCache.getCacheConfiguration().persistence().stores().get(0);
      assertTrue(config instanceof JpaStoreConfiguration);
      JpaStoreConfiguration jpaConfig = (JpaStoreConfiguration) config;
      assertEquals(1, jpaConfig.batchSize());
      assertEquals(KeyValueEntity.class, jpaConfig.entityClass());
   }
}
