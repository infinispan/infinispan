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

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.manager.DefaultCacheManager;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "config.ConfigurationUnitTest")
public class ConfigurationUnitTest {
   
   
   @Test
   public void testBuild() {
      // Simple test to ensure we can actually build a config
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.build();
   }
   
   @Test
   public void testCreateCache() {
      new DefaultCacheManager(new ConfigurationBuilder().build());
   }
   
   @Test
   public void testAdapt() {
      // Simple test to ensure we can actually adapt a config to the old config
      ConfigurationBuilder cb = new ConfigurationBuilder();
      new LegacyConfigurationAdaptor().adapt(cb.build());
   }
   
   @Test
   public void testEvictionMaxEntries() {
      Configuration configuration = new ConfigurationBuilder()
         .eviction().maxEntries(20)
         .build();
      org.infinispan.config.Configuration legacy = new LegacyConfigurationAdaptor().adapt(configuration);
      Assert.assertEquals(legacy.getEvictionMaxEntries(), 20);
   }
   
   @Test
   public void testDistSyncAutoCommit() {
      Configuration configuration = new ConfigurationBuilder()
         .clustering().cacheMode(CacheMode.DIST_SYNC)
         .transaction().autoCommit(true)
         .build();
      org.infinispan.config.Configuration legacy = new LegacyConfigurationAdaptor().adapt(configuration);
      Assert.assertTrue(legacy.isTransactionAutoCommit());
      Assert.assertEquals(org.infinispan.config.Configuration.CacheMode.DIST_SYNC.name(), CacheMode.DIST_SYNC.name());
   }
   
   

}
