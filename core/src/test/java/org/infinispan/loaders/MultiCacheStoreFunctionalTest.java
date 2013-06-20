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

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
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
      List<ConfigurationBuilder> configs = configs(2, m);
      EmbeddedCacheManager cacheManager0 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(0));
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(1));
      try {
         Cache<String, String> cache0 = cacheManager0.getCache();
         Cache<String, String> cache1 = cacheManager1.getCache();
         TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, cache1);

         cache0.put("KEY", "VALUE V1");
         assertEquals("VALUE V1", cache1.get("KEY"));

         TestingUtil.killCacheManagers(cacheManager1);
         cacheManager1 = null;
         cache1 = null;
         TestingUtil.blockUntilViewsChanged(TIMEOUT, 1, cache0);

         cache0.put("KEY", "VALUE V2");
         assertEquals("VALUE V2", cache0.get("KEY"));

         cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(1));
         cache1 = cacheManager1.getCache();
         TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, cache1);

         assertEquals("VALUE V2", cache1.get("KEY"));
      } finally {
         TestingUtil.killCacheManagers(cacheManager0, cacheManager1);
      }
   }

   /**
    * 
    * When node that persisted KEY2 it will resurrect previous value of KEY2.
    * 
    */
   public void testStartStopOfBackupResurrectsDeletedKey(Method m) throws Exception {
      List<ConfigurationBuilder> configs = configs(2, m);
      EmbeddedCacheManager cacheManager0 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(0));
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(1));
      try {
         Cache<String, String> cache0 = cacheManager0.getCache();
         Cache<String, String> cache1 = cacheManager1.getCache();
         TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, cache1);

         cache0.put("KEY2", "VALUE2 V1");
         assertEquals("VALUE2 V1", cache1.get("KEY2"));

         TestingUtil.killCacheManagers(cacheManager1);
         cacheManager1 = null;
         cache1 = null;
         TestingUtil.blockUntilViewsChanged(TIMEOUT, 1, cache0);

         cache0.put("KEY2", "VALUE2 V2");
         assertEquals("VALUE2 V2", cache0.get("KEY2"));
         cache0.remove("KEY2");
         assertEquals(null, cache0.get("KEY2"));

         cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(1));
         cache1 = cacheManager1.getCache();
         TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, cache1);

         assertEquals("VALUE2 V1", cache1.get("KEY2"));
      } finally {
         TestingUtil.killCacheManagers(cacheManager0, cacheManager1);
      }
   }

   /**
    * 
    * 1. start {n0, n1, n2} owners(k1) = {n0, n1} 2. stop n1 3. rewrite k1 value 4. start n1 5.
    * check k1 value is the new one in all nodes
    * 
    * @throws Exception
    */
   public void testStartStopOfBackupDoesntRewriteValue2(Method m) throws Exception {
      List<ConfigurationBuilder> configs = configs(3, m);
      EmbeddedCacheManager cacheManager0 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(0));
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(1));
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(2));
      try {
         Cache<MagicKey, String> cache0 = cacheManager0.getCache();
         Cache<MagicKey, String> cache1 = cacheManager1.getCache();
         Cache<MagicKey, String> cache2 = cacheManager2.getCache();

         TestingUtil.blockUntilViewsChanged(TIMEOUT, 3, cache0, cache1, cache2);

         MagicKey k1 = new MagicKey(cache0, cache1);

         cache0.put(k1, "VALUE V1");
         assert DistributionTestHelper.isOwner(cache0, k1);
         assert DistributionTestHelper.isOwner(cache1, k1);
         assert !DistributionTestHelper.isOwner(cache2, k1);

         assertEquals("VALUE V1", cache0.get(k1)); // primary owner
         assertEquals("VALUE V1", cache1.get(k1)); // secondary owner
         assertEquals("VALUE V1", cache2.get(k1)); // remotely fetched

         TestingUtil.killCacheManagers(cacheManager1);
         cacheManager1 = null;
         cache1 = null;

         TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, cache2);
         assert DistributionTestHelper.isOwner(cache0, k1);
         assert DistributionTestHelper.isOwner(cache2, k1);

         cache0.put(k1, "VALUE V2");
         assertEquals("VALUE V2", cache0.get(k1));
         assertEquals("VALUE V2", cache2.get(k1));

         cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(1));
         cache1 = cacheManager1.getCache();
         TestingUtil.blockUntilViewsChanged(TIMEOUT, 3, cache0, cache1, cache2);

         assertEquals("VALUE V2", cache0.get(k1));
         assertEquals("VALUE V2", cache1.get(k1));
         assertEquals("VALUE V2", cache2.get(k1));
      } finally {
         TestingUtil.killCacheManagers(cacheManager0, cacheManager1, cacheManager2);
      }
   }

   /**
    * 
    * 1. start {n0, n1, n2} owners(k1) = {n0, n1} 2. stop n1 3. rewrite k1 value 4. start n3 5.
    * start n1 check k1 value is the new one in all nodes
    * 
    * @throws Exception
    */
   public void testStartStopOfBackupDoesntRewriteValue3(Method m) throws Exception {
      List<ConfigurationBuilder> configs = configs(4, m);
      EmbeddedCacheManager cacheManager0 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(0));
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(1));
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(2));
      EmbeddedCacheManager cacheManager3 = null;
      Cache<MagicKey, String> cache3 = null; // soon
      try {
         Cache<MagicKey, String> cache0 = cacheManager0.getCache();
         Cache<MagicKey, String> cache1 = cacheManager1.getCache();
         Cache<MagicKey, String> cache2 = cacheManager2.getCache();

         TestingUtil.blockUntilViewsChanged(TIMEOUT, 3, cache0, cache1, cache2);

         MagicKey k1 = new MagicKey(cache0, cache1);

         cache0.put(k1, "VALUE V1");
         assert DistributionTestHelper.isOwner(cache0, k1);
         assert DistributionTestHelper.isOwner(cache1, k1);
         assert !DistributionTestHelper.isOwner(cache2, k1);

         assertEquals("VALUE V1", cache0.get(k1)); // primary owner
         assertEquals("VALUE V1", cache1.get(k1)); // secondary owner
         assertEquals("VALUE V1", cache2.get(k1)); // remotely fetched

         TestingUtil.killCacheManagers(cacheManager1);
         cacheManager1 = null;
         cache1 = null;

         TestingUtil.blockUntilViewsChanged(TIMEOUT, 2, cache0, cache2);
         assert DistributionTestHelper.isOwner(cache0, k1);
         assert DistributionTestHelper.isOwner(cache2, k1);

         cache0.put(k1, "VALUE V2");
         assertEquals("VALUE V2", cache0.get(k1));
         assertEquals("VALUE V2", cache2.get(k1));

         cacheManager3 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(3));
         cache3 = cacheManager3.getCache();
         TestingUtil.blockUntilViewsChanged(TIMEOUT, 3, cache0, cache2, cache3);

         assertEquals("VALUE V2", cache0.get(k1));
         assertEquals("VALUE V2", cache2.get(k1));
         assertEquals("VALUE V2", cache3.get(k1));

         cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(configs.get(1));
         cache1 = cacheManager1.getCache();
         TestingUtil.blockUntilViewsChanged(TIMEOUT, 4, cache0, cache1, cache2, cache3);

         assertEquals("VALUE V2", cache0.get(k1));
         assertEquals("VALUE V2", cache1.get(k1));
         assertEquals("VALUE V2", cache2.get(k1));
         assertEquals("VALUE V2", cache3.get(k1));
      } finally {
         TestingUtil.killCacheManagers(cacheManager0, cacheManager1, cacheManager2);
      }
   }

}
