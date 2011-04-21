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
package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.lifecycle.ComponentStatus;
import org.testng.annotations.Test;

import java.util.Set;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerTest")
public class CacheManagerTest extends AbstractInfinispanTest {
   public void testDefaultCache() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager();

      try {
         assert cm.getCache().getStatus() == ComponentStatus.RUNNING;
         assert cm.getCache().getName().equals(CacheContainer.DEFAULT_CACHE_NAME);

         try {
            cm.defineConfiguration(CacheContainer.DEFAULT_CACHE_NAME, new Configuration());
            assert false : "Should fail";
         }
         catch (IllegalArgumentException e) {
            assert true; // ok
         }
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testUnstartedCachemanager() {
      DefaultCacheManager dcm = new DefaultCacheManager(false);
      assert dcm.getStatus().equals(ComponentStatus.INSTANTIATED);
      assert !dcm.getStatus().allowInvocations();
      Cache<Object, Object> cache = dcm.getCache();
      cache.put("k","v");
      assert cache.get("k").equals("v");
   }

   public void testClashingNames() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Configuration c = new Configuration();
         Configuration firstDef = cm.defineConfiguration("aCache", c);
         Configuration secondDef = cm.defineConfiguration("aCache", c);
         assert firstDef.equals(secondDef);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testStartAndStop() {
      CacheContainer cm = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Cache c1 = cm.getCache("cache1");
         Cache c2 = cm.getCache("cache2");
         Cache c3 = cm.getCache("cache3");

         assert c1.getStatus() == ComponentStatus.RUNNING;
         assert c2.getStatus() == ComponentStatus.RUNNING;
         assert c3.getStatus() == ComponentStatus.RUNNING;

         cm.stop();

         assert c1.getStatus() == ComponentStatus.TERMINATED;
         assert c2.getStatus() == ComponentStatus.TERMINATED;
         assert c3.getStatus() == ComponentStatus.TERMINATED;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testDefiningConfigurationValidation() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      try {
         cm.defineConfiguration("cache1", null);
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
      
      try {
         cm.defineConfiguration(null, null);
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
      
      try {
         cm.defineConfiguration(null, new Configuration());
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
      
      Configuration c = cm.defineConfiguration("cache1", null, new Configuration());
      assert c.equals(cm.getDefaultConfiguration());
      
      c = cm.defineConfiguration("cache1", "does-not-exist-cache", new Configuration());
      assert c.equals(cm.getDefaultConfiguration());
   }

   public void testDefiningConfigurationWithTemplateName() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager();

      Configuration c = new Configuration();
      c.setIsolationLevel(IsolationLevel.NONE);
      Configuration oneCacheConfiguration = cm.defineConfiguration("oneCache", c);
      assert oneCacheConfiguration.equals(c);
      assert oneCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      
      c = new Configuration();
      Configuration secondCacheConfiguration = cm.defineConfiguration("secondCache", "oneCache", c);
      assert oneCacheConfiguration.equals(secondCacheConfiguration);
      assert secondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      
      c = new Configuration();
      c.setIsolationLevel(IsolationLevel.SERIALIZABLE);
      Configuration anotherSecondCacheConfiguration = cm.defineConfiguration("secondCache", "oneCache", c);
      assert !secondCacheConfiguration.equals(anotherSecondCacheConfiguration);
      assert anotherSecondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.SERIALIZABLE);
      assert secondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      
      c = new Configuration();
      c.setExpirationMaxIdle(Long.MAX_VALUE);
      Configuration yetAnotherSecondCacheConfiguration = cm.defineConfiguration("secondCache", "oneCache", c);
      assert yetAnotherSecondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      assert yetAnotherSecondCacheConfiguration.getExpirationMaxIdle() == Long.MAX_VALUE;
      assert secondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      assert anotherSecondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.SERIALIZABLE);
   }

   public void testDefiningConfigurationOverridingBooleans() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      Configuration c = new Configuration();
      c.setUseLazyDeserialization(true);
      Configuration lazy = cm.defineConfiguration("lazyDeserialization", c);
      assert lazy.isUseLazyDeserialization();

      c = new Configuration();
      c.setEvictionStrategy(EvictionStrategy.LRU);
      Configuration lazyLru = cm.defineConfiguration("lazyDeserializationWithLRU", "lazyDeserialization", c);
      assert lazyLru.isUseLazyDeserialization();
      assert lazyLru.getEvictionStrategy() == EvictionStrategy.LRU;
   }

   public void testGetCacheNames() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      try {
         cm.defineConfiguration("one", new Configuration());
         cm.defineConfiguration("two", new Configuration());
         cm.getCache("three");
         Set<String> cacheNames = cm.getCacheNames();
         assert 3 == cacheNames.size();
         assert cacheNames.contains("one");
         assert cacheNames.contains("two");
         assert cacheNames.contains("three");
      } finally {
         cm.stop();
      }
   }

   public void testCacheStopTwice() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Cache cache = localCacheManager.getCache();
         cache.put("k", "v");
         cache.stop();
         cache.stop();
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   public void testCacheManagerStopTwice() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Cache cache = localCacheManager.getCache();
         cache.put("k", "v");
         localCacheManager.stop();
         localCacheManager.stop();
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testCacheStopManagerStopFollowedByGetCache() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Cache cache = localCacheManager.getCache();
         cache.put("k", "v");
         cache.stop();
         localCacheManager.stop();
         localCacheManager.getCache();
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testCacheStopManagerStopFollowedByCacheOp() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Cache cache = localCacheManager.getCache();
         cache.put("k", "v");
         cache.stop();
         localCacheManager.stop();
         cache.put("k", "v2");
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }
   
}
