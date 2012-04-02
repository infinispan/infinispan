/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.eviction;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;


@Test(groups = "functional", testName = "eviction.EvictionWithPassivationTest")
public class EvictionWithPassivationTest extends SingleCacheManagerTest {

   private Configuration buildCfg(EvictionThreadPolicy threadPolicy, EvictionStrategy strategy) {
      Configuration cfg = new Configuration();
      CacheStoreConfig cacheStoreConfig = new DummyInMemoryCacheStore.Cfg();
      cacheStoreConfig.setPurgeOnStartup(true);
      cfg.getCacheLoaderManagerConfig().addCacheLoaderConfig(cacheStoreConfig);
      cfg.getCacheLoaderManagerConfig().setPassivation(true);
      cfg.setEvictionStrategy(strategy);
      cfg.setEvictionThreadPolicy(threadPolicy);
      cfg.setEvictionMaxEntries(2);
      cfg.setInvocationBatchingEnabled(true);
      return cfg;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(getDefaultStandaloneConfig(true));

      for (EvictionStrategy s : EvictionStrategy.values()) {
         for (EvictionThreadPolicy p : EvictionThreadPolicy.values()) {
            cacheManager.defineConfiguration("test-" + p + "-" + s, buildCfg(p, s));
         }
      }

      return cacheManager;
   }

   public void testPiggybackLRU() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LRU);
   }

   
   public void testPiggybackLIRS() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.LIRS);
   }

   public void testPiggybackNONE() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.NONE);
   }

   public void testPiggybackUNORDERED() {
      runTest(EvictionThreadPolicy.PIGGYBACK, EvictionStrategy.UNORDERED);
   }

   public void testDefaultLRU() {
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LRU);
   }

   
   public void testDefaultLIRS() {
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.LIRS);
   }

   public void testDefaultNONE() {
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.NONE);
   }

   public void testDefaultUNORDERED() {
      runTest(EvictionThreadPolicy.DEFAULT, EvictionStrategy.UNORDERED);
   }

   private void runTest(EvictionThreadPolicy p, EvictionStrategy s) {
      String name = "test-" + p + "-" + s;
      Cache<String, String> testCache = cacheManager.getCache(name);
      testCache.clear();
      testCache.put("X", "4567");
      testCache.put("Y", "4568");
      testCache.put("Z", "4569");

      assert "4567".equals( testCache.get("X") ) : "Failure on test " + name;
      assert "4568".equals( testCache.get("Y") ) : "Failure on test " + name;
      assert "4569".equals( testCache.get("Z") ) : "Failure on test " + name;

      for (int i=0; i<10; i++) {
         testCache.getAdvancedCache().startBatch();
         String k = "A"+i;
         testCache.put(k, k);
         k = "B"+i;
         testCache.put(k, k);
         testCache.getAdvancedCache().endBatch(true);
      }

      for (int i=0; i<10; i++) {
         String k = "A"+i;
         assert k.equals(testCache.get(k)) : "Failure on test " + name;
         k = "B"+i;
         assert k.equals(testCache.get(k)) : "Failure on test " + name;
      }
   }

}