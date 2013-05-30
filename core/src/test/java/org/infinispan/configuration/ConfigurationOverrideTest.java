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
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.infinispan.eviction.EvictionStrategy.*;
import static org.infinispan.configuration.cache.CacheMode.*;

@Test(groups = "functional", testName = "configuration.ConfigurationOverrideTest")
public class ConfigurationOverrideTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cm;

   @AfterMethod
   public void stopCacheManager() {
      cm.stop();
   }

   public void testConfigurationOverride() throws Exception {
      ConfigurationBuilder defaultCfgBuilder = new ConfigurationBuilder();
      defaultCfgBuilder.eviction().maxEntries(200).strategy(LIRS);

      cm = TestCacheManagerFactory.createCacheManager(defaultCfgBuilder);
      final ConfigurationBuilder cacheCfgBuilder =
            new ConfigurationBuilder().read(defaultCfgBuilder.build());
      cm.defineConfiguration("my-cache", cacheCfgBuilder.build());
      Cache<?, ?> cache = cm.getCache("my-cache");
      assertEquals(200,
            cache.getCacheConfiguration().eviction().maxEntries());
      assertEquals(LIRS,
            cache.getCacheConfiguration().eviction().strategy());
   }

   public void testOldConfigurationOverride() throws Exception {
      org.infinispan.config.Configuration defaultConfiguration =
            new org.infinispan.config.Configuration().fluent()
            .eviction().maxEntries(200).strategy(LIRS)
            .build();

      cm = TestCacheManagerFactory.createCacheManager(defaultConfiguration);
      final org.infinispan.config.Configuration cacheConfiguration =
            new org.infinispan.config.Configuration().fluent().build();
      cm.defineConfiguration("my-cache", cacheConfiguration);
      Cache<?, ?> cache = cm.getCache("my-cache");
      org.infinispan.config.Configuration cfg = cache.getConfiguration();
      assertEquals(200, cfg.getEvictionMaxEntries());
      assertEquals(LIRS, cfg.getEvictionStrategy());
   }

   public void testSimpleDistributedClusterModeDefault() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(DIST_SYNC)
            .hash().numOwners(3).numSegments(51);

      cm = TestCacheManagerFactory.createClusteredCacheManager(builder);

      Cache<?, ?> cache = cm.getCache("my-cache");
      // These are all overridden values
      ClusteringConfiguration clusteringCfg =
            cache.getCacheConfiguration().clustering();
      assertEquals(DIST_SYNC, clusteringCfg.cacheMode());
      assertEquals(3, clusteringCfg.hash().numOwners());
      assertEquals(51, clusteringCfg.hash().numSegments());
   }

   public void testSimpleDistributedClusterModeNamedCache() throws Exception {
      final String cacheName = "my-cache";
      final Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(DIST_SYNC)
            .hash().numOwners(3).numSegments(51).build();

      cm = TestCacheManagerFactory.createClusteredCacheManager();
      cm.defineConfiguration(cacheName, config);
      Cache<?, ?> cache = cm.getCache(cacheName);
      ClusteringConfiguration clusteringCfg =
            cache.getCacheConfiguration().clustering();
      assertEquals(DIST_SYNC, clusteringCfg.cacheMode());
      assertEquals(3, clusteringCfg.hash().numOwners());
      assertEquals(51, clusteringCfg.hash().numSegments());
   }

   public void testOverrideWithStore() {
      final ConfigurationBuilder builder1 = new ConfigurationBuilder();
      builder1.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class);
      cm = new DefaultCacheManager(new GlobalConfigurationBuilder().build(), builder1.build());
      ConfigurationBuilder builder2 = new ConfigurationBuilder();
      builder2.read(cm.getDefaultCacheConfiguration());
      builder2.eviction().maxEntries(1000);
      Configuration configuration = cm.defineConfiguration("named", builder2.build());
      assertEquals(1, configuration.loaders().cacheLoaders().size());
   }

}