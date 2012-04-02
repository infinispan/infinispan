/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.configuration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional")
public class ConfigurationOverrideTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cm;

   @AfterMethod
   public void stopCacheManager() {
      cm.stop();
   }

   public void testConfigurationOverride() {
      Configuration defaultConfiguration = new ConfigurationBuilder()
            .eviction().maxEntries(200).strategy(EvictionStrategy.LIRS)
            .build();

      Configuration cacheConfiguration = new ConfigurationBuilder().read(defaultConfiguration).build();

      cm = new DefaultCacheManager(defaultConfiguration);
      cm.defineConfiguration("my-cache", cacheConfiguration);

      Cache<?, ?> cache = cm.getCache("my-cache");

      Assert.assertEquals(cache.getCacheConfiguration().eviction().maxEntries(), 200);
      Assert.assertEquals(cache.getCacheConfiguration().eviction().strategy(), EvictionStrategy.LIRS);
   }

   public void testOldConfigurationOverride() {
      org.infinispan.config.Configuration defaultConfiguration = new org.infinispan.config.Configuration().fluent()
            .eviction().maxEntries(200).strategy(EvictionStrategy.LIRS)
            .build();

      org.infinispan.config.Configuration cacheConfiguration = new org.infinispan.config.Configuration().fluent()
            .build();

      cm = new DefaultCacheManager(defaultConfiguration);
      cm.defineConfiguration("my-cache", cacheConfiguration);

      Cache<?, ?> cache = cm.getCache("my-cache");

      Assert.assertEquals(cache.getConfiguration().getEvictionMaxEntries(), 200);
      Assert.assertEquals(cache.getConfiguration().getEvictionStrategy(), EvictionStrategy.LIRS);
   }
   
   public void testSimpleDistributedClusterModeDefault() {
      Configuration config = 
            new ConfigurationBuilder()
               .clustering()
                  .cacheMode(CacheMode.DIST_SYNC)
                  .hash()
                     .numOwners(3)
                     .numVirtualNodes(51)
             .build();
      


      cm = TestCacheManagerFactory.createCacheManager(config);

      Cache<?, ?> cache = cm.getCache("my-cache");
      
      // These are all overridden values
      Assert.assertEquals(cache.getCacheConfiguration().clustering().cacheMode(), CacheMode.DIST_SYNC);
      Assert.assertEquals(cache.getCacheConfiguration().clustering().hash().numOwners(), 3);
      Assert.assertEquals(cache.getCacheConfiguration().clustering().hash().numVirtualNodes(), 51);
   }
   
   public void testSimpleDistributedClusterModeNamedCache() {
      String cacheName = "my-cache";
      Configuration config = 
            new ConfigurationBuilder()
               .clustering()
                  .cacheMode(CacheMode.DIST_SYNC)
                  .hash()
                     .numOwners(3)
                     .numVirtualNodes(51)
             .build();

      cm = TestCacheManagerFactory.createCacheManager(config);

      cm.defineConfiguration(cacheName, config);
      
      Cache<?, ?> cache = cm.getCache(cacheName);
      
      // These are all overridden values
      Assert.assertEquals(cache.getCacheConfiguration().clustering().cacheMode(), CacheMode.DIST_SYNC);
      Assert.assertEquals(cache.getCacheConfiguration().clustering().hash().numOwners(), 3);
      Assert.assertEquals(cache.getCacheConfiguration().clustering().hash().numVirtualNodes(), 51);
   }
}