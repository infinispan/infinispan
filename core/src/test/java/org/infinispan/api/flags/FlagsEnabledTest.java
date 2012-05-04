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
package org.infinispan.api.flags;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.SKIP_CACHE_STORE;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.test.TestingUtil.withTx;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.Flag;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.UnnnecessaryLoadingTest.CountingCacheStore;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "api.flags.FlagsEnabledTest")
@CleanupAfterMethod
public class FlagsEnabledTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder
            .locking().writeSkewCheck(true).isolationLevel(IsolationLevel.REPEATABLE_READ)
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .loaders().addCacheLoader().cacheLoader(new CountingCacheStore())
            .loaders().addCacheLoader().cacheLoader(new DummyInMemoryCacheStore())
            .transaction().syncCommitPhase(true);
      createClusteredCaches(2, "replication", builder);
   }

   CountingCacheStore getCacheStore(Cache cache) {
      CacheLoaderManager clm = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      ChainingCacheStore ccs = (ChainingCacheStore) clm.getCacheLoader();
      CountingCacheStore countingCS = (CountingCacheStore) ccs.getStores().keySet().iterator().next();
      return countingCS;
   }

   public void testWithFlagsSemantics() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;

      AdvancedCache cache1LocalOnly = cache1.withFlags(CACHE_MODE_LOCAL);
      cache1LocalOnly.put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");
      
      assert getCacheStore(cache1).numLoads == 1;
      assert getCacheStore(cache2).numLoads == 1;
      assert getCacheStore(cache2) != getCacheStore(cache1);
      
      cache1.put("nonLocal", "value");
      assert "value".equals(cache2.get("nonLocal"));
      assert getCacheStore(cache1).numLoads == 2;
      assert getCacheStore(cache2).numLoads == 1; //not incremented since ISPN-1642
      
      AdvancedCache cache1SkipRemoteAndStores = cache1LocalOnly.withFlags(SKIP_CACHE_STORE);
      cache1SkipRemoteAndStores.put("again", "value");
      assert getCacheStore(cache1).numLoads == 2;
      assert getCacheStore(cache2).numLoads == 1;
      assert cache1.get("again").equals("value");
      assert cache2.get("again") == null;

      assert getCacheStore(cache1).numLoads == 2;
      assert getCacheStore(cache2).numLoads == 2; //"again" wasn't found in cache, looks into store
      
      assert cache2.get("again") == null;
      assert getCacheStore(cache2).numLoads == 3;
      assert cache2.withFlags(SKIP_CACHE_STORE).get("again") == null;
      assert getCacheStore(cache2).numLoads == 3;
      
      assert getCacheStore(cache1).numLoads == 2;
      assert cache1LocalOnly.get("localStored") == null;
      assert getCacheStore(cache1).numLoads == 3; //options on cache1SkipRemoteAndStores did NOT affect this cache
   }

   public void testReplicateSkipCacheLoad(Method m) {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;

      final String v = v(m, 1);
      final String k = k(m, 1);
      cache1.withFlags(Flag.SKIP_CACHE_LOAD).put(k, v);
      assert v.equals(cache2.get(k));

      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;
   }

   public void testReplicateSkipCacheLoaderWithinTxInCoordinator(Method m) throws Exception {
      final AdvancedCache cache1 = cache(0, "replication").getAdvancedCache();
      final AdvancedCache cache2 = cache(1, "replication").getAdvancedCache();
      doReplicateSkipCacheLoaderWithinTx(m, cache1, cache2);
   }

   public void testReplicateSkipCacheLoaderWithinTxInNonCoordinator(Method m) throws Exception {
      final AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      final AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      doReplicateSkipCacheLoaderWithinTx(m, cache2, cache1);
   }

   private void doReplicateSkipCacheLoaderWithinTx(Method m, final AdvancedCache cache1, AdvancedCache cache2) throws Exception {
      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;

      final String v = v(m, 1);
      final String k = k(m, 1);
      withTx(cache1.getTransactionManager(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache1.withFlags(Flag.SKIP_CACHE_LOAD).put(k, v);
            return null;
         }
      });

      assert v.equals(cache2.get(k));
      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;
   }

}
