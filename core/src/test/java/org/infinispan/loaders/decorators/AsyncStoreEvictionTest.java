/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.decorators;

import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CacheStoreConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.decorators.AsyncTest")
public class AsyncStoreEvictionTest {

   // set to false to fix all the tests
   private static final boolean USE_ASYNC_STORE = true;

   private static ConfigurationBuilder config(boolean passivation, int threads) {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.expiration().wakeUpInterval(100);
      config.eviction().maxEntries(1).strategy(EvictionStrategy.LRU);
      CacheStoreConfigurationBuilder store = config.loaders().passivation(passivation).addStore().cacheStore(new LockableCacheStore());
      if (USE_ASYNC_STORE)
         store.async().enable().threadPoolSize(threads);
      return config;
   }

   private final static ThreadLocal<LockableCacheStore> STORE = new ThreadLocal<LockableCacheStore>();

   public static class LockableCacheStoreConfig extends DummyInMemoryCacheStore.Cfg {
      private static final long serialVersionUID = 1L;

      public LockableCacheStoreConfig() {
         setCacheLoaderClassName(LockableCacheStore.class.getName());
      }
   }

   @CacheLoaderMetadata(configurationClass = LockableCacheStoreConfig.class)
   public static class LockableCacheStore extends DummyInMemoryCacheStore {
      private final ReentrantLock lock = new ReentrantLock();

      public LockableCacheStore() {
         super();
         STORE.set(this);
      }

      @Override
      public Class<? extends CacheLoaderConfig> getConfigurationClass() {
         return LockableCacheStoreConfig.class;
      }

      @Override
      public void store(InternalCacheEntry ed) {
         lock.lock();
         try {
            super.store(ed);
         } finally {
            lock.unlock();
         }
      }

      @Override
      public boolean remove(Object key) {
         lock.lock();
         try {
            return super.remove(key);
         } finally {
            lock.unlock();
         }
      }
   }

   private static abstract class CacheCallable extends CacheManagerCallable {
      protected final Cache<String, String> cache;
      protected final LockableCacheStore store;

      CacheCallable(ConfigurationBuilder builder) {
         super(TestCacheManagerFactory.createCacheManager(builder));
         cache = cm.getCache();
         store = STORE.get();
      }
   }

   public void testEndToEndEvictionPassivation() throws Exception {
      testEndToEndEviction(true);
   }
   public void testEndToEndEviction() throws Exception {
      testEndToEndEviction(false);
   }
   private void testEndToEndEviction(boolean passivation) throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(passivation, 1)) {
         @Override
         public void call() {
            // simulate slow back end store
            store.lock.lock();
            try {
               cache.put("k1", "v1");
               cache.put("k2", "v2"); // force eviction of "k1"
               TestingUtil.sleepThread(100); // wait until the only AsyncProcessor thread is blocked
               cache.put("k3", "v3");
               cache.put("k4", "v4"); // force eviction of "k3"

               assert "v3".equals(cache.get("k3")) : "cache must return k3 == v3 (was: " + cache.get("k3") + ")";
            } finally {
               store.lock.unlock();
            }
         }
      });
   }

   public void testEndToEndUpdatePassivation() throws Exception {
      testEndToEndUpdate(true);
   }
   public void testEndToEndUpdate() throws Exception {
      testEndToEndUpdate(false);
   }
   private void testEndToEndUpdate(boolean passivation) throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(passivation, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v0");
            cache.put("k2", "v2"); // force eviction of "k1"

            // wait for k1 == v1 to appear in store
            while (store.load("k1") == null)
               TestingUtil.sleepThread(10);

            // simulate slow back end store
            store.lock.lock();
            try {
               cache.put("k3", "v3");
               cache.put("k4", "v4"); // force eviction of "k3"
               TestingUtil.sleepThread(100); // wait until the only AsyncProcessor thread is blocked
               cache.put("k1", "v1");
               cache.put("k5", "v5"); // force eviction of "k1"

               assert "v1".equals(cache.get("k1")) : "cache must return k1 == v1 (was: " + cache.get("k1") + ")";
            } finally {
               store.lock.unlock();
            }
         }
      });
   }

   public void testEndToEndRemovePassivation() throws Exception {
      testEndToEndRemove(true);
   }
   public void testEndToEndRemove() throws Exception {
      testEndToEndRemove(false);
   }
   private void testEndToEndRemove(boolean passivation) throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(passivation, 2)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.put("k2", "v2"); // force eviction of "k1"

            // wait for "k1" to appear in store
            while (store.load("k1") == null)
               TestingUtil.sleepThread(10);

            // simulate slow back end store
            store.lock.lock();
            try {
               cache.remove("k1");
               TestingUtil.sleepThread(100); // wait until the first AsyncProcessor thread is blocked
               cache.remove("k1"); // make second AsyncProcessor thread burn asyncProcessorIds
               TestingUtil.sleepThread(200); // wait for reaper to collect InternalNullEntry

               assert null == cache.get("k1") : "cache must return k1 == null (was: " + cache.get("k1") + ")";
            } finally {
               store.lock.unlock();
            }
         }
      });
   }

   public void testNPE() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.remove("k1");
            // this causes NPE in AsyncStore.isLocked(InternalNullEntry.getKey())
            cache.put("k2", "v2");
         }
      });
   }

   public void testLIRS() throws Exception {
      ConfigurationBuilder config = config(false, 1);
      config.eviction().strategy(EvictionStrategy.LIRS).maxEntries(1);
      TestingUtil.withCacheManager(new CacheCallable(config) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            cache.put("k1", "v3");
            cache.put("k2", "v4");
            cache.put("k3", "v3");
            cache.put("k4", "v4");
         }
      });
   }

   public void testSize() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.put("k2", "v2");

            assert cache.size() == 1 : "cache size must be 1, was: " + cache.size();
         }
      });
   }

   public void testSizeAfterExpiration() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            TestingUtil.sleepThread(200);

            assert !(cache.size() == 2) : "expiry doesn't work even after expiration";
         }
      });
   }

   public void testSizeAfterEvict() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.evict("k1");

            assert cache.size() == 0 : "cache size must be 0, was: " + cache.size();
         }
      });
   }

   public void testSizeAfterRemove() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.remove("k1");

            assert cache.size() == 0 : "cache size must be 0, was: " + cache.size();
         }
      });
   }

   public void testSizeAfterRemoveAndExpiration() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.remove("k1");
            int size = cache.size();
            TestingUtil.sleepThread(200);

            assert !(size == 1 && cache.size() == 0) : "remove only works after expiration";
         }
      });
   }
}
