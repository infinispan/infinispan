package org.infinispan.test.integration.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.test.integration.data.KeyValueEntity;
import org.infinispan.test.integration.protostream.ServerIntegrationSCIImpl;
import org.junit.After;
import org.junit.Test;

/**
 * Test the Infinispan JPA CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public abstract class AbstractInfinispanStoreJpaIT {

   private EmbeddedCacheManager cm;

   @After
   public void cleanUp() {
      if (cm != null)
         cm.stop();
   }

   @Test
   public void testCacheManager() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.defaultCacheName("default");
      gcb.serialization().addContextInitializer(new ServerIntegrationSCIImpl());
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addStore(JpaStoreConfigurationBuilder.class)
            .persistenceUnitName("org.infinispan.persistence.jpa")
            .segmented(false)
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
