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
package org.infinispan.tx.recovery;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.RecoveryWithCustomCacheDistTest")
public class RecoveryWithCustomCacheDistTest extends RecoveryWithDefaultCacheDistTest {

   private static final String CUSTOM_CACHE = "customCache";

   private ConfigurationBuilder recoveryCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      configuration = super.configure();
      configuration.transaction().recovery().recoveryInfoCacheName(CUSTOM_CACHE);

      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(configuration));
      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(configuration));

      recoveryCache = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      recoveryCache.transaction().transactionManagerLookup(null);
      // Explicitly disable recovery in recovery cache per se.
      recoveryCache.transaction().recovery().disable();
      manager(0).defineConfiguration(CUSTOM_CACHE, recoveryCache.build());
      manager(1).defineConfiguration(CUSTOM_CACHE, recoveryCache.build());

      manager(0).startCaches(CacheContainer.DEFAULT_CACHE_NAME, CUSTOM_CACHE);
      manager(1).startCaches(CacheContainer.DEFAULT_CACHE_NAME, CUSTOM_CACHE);
      waitForClusterToForm(CUSTOM_CACHE);

      assert manager(0).getCacheNames().contains(CUSTOM_CACHE);
      assert manager(1).getCacheNames().contains(CUSTOM_CACHE);
   }

   @Override
   protected String getRecoveryCacheName() {
      return CUSTOM_CACHE;
   }

   @Override
   protected void defineRecoveryCache(int cacheManagerIndex) {
      manager(1).defineConfiguration(CUSTOM_CACHE, recoveryCache.build());
   }
}
