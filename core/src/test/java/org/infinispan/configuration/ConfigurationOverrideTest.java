package org.infinispan.configuration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional")
public class ConfigurationOverrideTest {

   public void testConfigurationOverride() {
      Configuration defaultConfiguration = new ConfigurationBuilder()
            .eviction().maxEntries(200).strategy(EvictionStrategy.FIFO)
            .build();

      Configuration cacheConfiguration = new ConfigurationBuilder().read(defaultConfiguration).build();

      EmbeddedCacheManager embeddedCacheManager = new DefaultCacheManager(defaultConfiguration);
      embeddedCacheManager.defineConfiguration("my-cache", cacheConfiguration);

      Cache<?, ?> cache = embeddedCacheManager.getCache("my-cache");

      Assert.assertEquals(cache.getCacheConfiguration().eviction().maxEntries(), 200);
      Assert.assertEquals(cache.getCacheConfiguration().eviction().strategy(), EvictionStrategy.FIFO);
   }

   public void testOldConfigurationOverride() {
      org.infinispan.config.Configuration defaultConfiguration = new org.infinispan.config.Configuration().fluent()
            .eviction().maxEntries(200).strategy(EvictionStrategy.FIFO)
            .build();

      org.infinispan.config.Configuration cacheConfiguration = new org.infinispan.config.Configuration().fluent()
            .build();

      EmbeddedCacheManager embeddedCacheManager = new DefaultCacheManager(defaultConfiguration);
      embeddedCacheManager.defineConfiguration("my-cache", cacheConfiguration);

      Cache<?, ?> cache = embeddedCacheManager.getCache("my-cache");

      Assert.assertEquals(cache.getConfiguration().getEvictionMaxEntries(), 200);
      Assert.assertEquals(cache.getConfiguration().getEvictionStrategy(), EvictionStrategy.FIFO);
   }
}