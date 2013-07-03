/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * This is a base functional test class for tests with multiple cache stores
 * 
 * @author Michal Linhard (mlinhard@redhat.com)
 */
@Test(groups = "unit", testName = "loaders.MultiCacheStoreFunctionalTest")
public abstract class MultiCacheStoreFunctionalTest<TStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<?, ?>> extends AbstractInfinispanTest {

   private static final long TIMEOUT = 10000;

   protected abstract TStoreConfigurationBuilder buildCacheStoreConfig(LoadersConfigurationBuilder builder, String discriminator) throws Exception;

   /**
    * 
    * Create n configs using cache store. sets passivation = false, purge = false, fetch persistent
    * state = true
    * 
    * @param n
    * @param method
    * @return
    * @throws Exception
    */
   protected List<ConfigurationBuilder> configs(int n, Method method) throws Exception {
      List<ConfigurationBuilder> r = new ArrayList<ConfigurationBuilder>(n);
      for (int i = 0; i < n; i++) {
         ConfigurationBuilder configBuilder = new ConfigurationBuilder();
         configBuilder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2);
         buildCacheStoreConfig(configBuilder.loaders().passivation(false), method.getName() + i).purgeOnStartup(false).fetchPersistentState(true);
         r.add(configBuilder);
      }
      return r;
   }

   /**
    * 
    * when a node that persisted KEY wakes up, it can't rewrite existing value.
    */
   public void testStartStopOfBackupDoesntRewriteValue(Method m) throws Exception {
      final List<ConfigurationBuilder> configs = configs(2, m);
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createClusteredCacheManager(configs.get(0)),
            TestCacheManagerFactory.createClusteredCacheManager(configs.get(1))) {
         @Override
         public void call() {
            final EmbeddedCacheManager cacheManager0 = cms[0];
            final EmbeddedCacheManager cacheManager1 = cms[1];
            final Cache<String, String> cache0 = cacheManager0.getCache();
            final Cache<String, String> cache1 = cacheManager1.getCache();
            TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, cache1);

            cache0.put("KEY", "VALUE V1");
            assertEquals("VALUE V1", cache1.get("KEY"));

            TestingUtil.killCacheManagers(cacheManager1);
            TestingUtil.blockUntilViewsChanged(TIMEOUT, 1, cache0);

            cache0.put("KEY", "VALUE V2");
            assertEquals("VALUE V2", cache0.get("KEY"));

            withCacheManager(new CacheManagerCallable(
                  TestCacheManagerFactory.createClusteredCacheManager(configs.get(1))) {
               @Override
               public void call() {
                  Cache<String, String> newCache = cm.getCache();
                  TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, newCache);
                  assertEquals("VALUE V2", newCache.get("KEY"));
               }
            });
         }
      });

   }

   /**
    * 
    * When node that persisted KEY2 it will resurrect previous value of KEY2.
    * 
    */
   public void testStartStopOfBackupResurrectsDeletedKey(Method m) throws Exception {
      final List<ConfigurationBuilder> configs = configs(2, m);
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createClusteredCacheManager(configs.get(0)),
            TestCacheManagerFactory.createClusteredCacheManager(configs.get(1))) {
         @Override
         public void call() {
            final EmbeddedCacheManager cacheManager0 = cms[0];
            final EmbeddedCacheManager cacheManager1 = cms[1];
            final Cache<String, String> cache0 = cacheManager0.getCache();
            final Cache<String, String> cache1 = cacheManager1.getCache();
            TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, cache1);

            cache0.put("KEY2", "VALUE2 V1");
            assertEquals("VALUE2 V1", cache1.get("KEY2"));

            TestingUtil.killCacheManagers(cacheManager1);
            TestingUtil.blockUntilViewsChanged(TIMEOUT, 1, cache0);

            cache0.put("KEY2", "VALUE2 V2");
            assertEquals("VALUE2 V2", cache0.get("KEY2"));
            cache0.remove("KEY2");
            assertEquals(null, cache0.get("KEY2"));

            withCacheManager(new CacheManagerCallable(
                  TestCacheManagerFactory.createClusteredCacheManager(configs.get(1))) {
               @Override
               public void call() {
                  Cache<String, String> newCache = cm.getCache();
                  TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, newCache);
                  assertEquals("VALUE2 V1", newCache.get("KEY2"));
               }
            });
         }
      });
   }

}
