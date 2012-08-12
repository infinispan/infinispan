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
package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistCacheStoreTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Arrays;

import static java.lang.String.format;

/**
 * Should ensure that persistent state is not rehashed if the cache store is shared.  See ISPN-861
 *
 */
@Test (testName = "distribution.rehash.RehashWithSharedCacheStore", groups = "functional")
public class RehashWithSharedCacheStore extends BaseDistCacheStoreTest {

   private static final Log log = LogFactory.getLog(RehashWithSharedCacheStore.class);

   public RehashWithSharedCacheStore() {
      INIT_CLUSTER_SIZE = 3;
      sync = true;
      tx = false;
      testRetVals = true;
      shared = true;
      performRehashing = true;
   }

   private CacheStore getCacheStore(Cache<?,?> cache) {
      CacheLoaderManager clm = cache.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      return clm.getCacheStore();
   }

   private int getCacheStoreStats(Cache<?, ?> cache, String cacheStoreMethod) {
      DummyInMemoryCacheStore cs = (DummyInMemoryCacheStore) getCacheStore(cache);
      return cs.stats().get(cacheStoreMethod);
   }

   public void testRehashes() throws CacheLoaderException {
      MagicKey k = new MagicKey("k", c1);

      c1.put(k, "v");

      Cache<Object, String>[] owners = getOwners(k);
      log.infof("Initial owners list for key %s: %s", k, Arrays.asList(owners));

      // Ensure the loader is shared!
      for (Cache<Object, String> c: Arrays.asList(c1, c2, c3)) {
         assert getCacheStore(c).containsKey(k) : format("CacheStore on %s should contain key %s", c, k);
      }

      Cache<Object, String> primaryOwner = owners[0];
      if (getCacheStoreStats(primaryOwner, "store") == 0) primaryOwner = owners[1];

      for (Cache<Object, String> c: owners) {
         int numWrites = getCacheStoreStats(c, "store");
         assert numWrites == 1 : "store() should have been invoked on the cache store once.  Was " + numWrites;
      }

      log.infof("Stopping node %s", primaryOwner);

      caches.remove(primaryOwner);
      primaryOwner.stop();
      primaryOwner.getCacheManager().stop();


      TestingUtil.blockUntilViewsReceived(60000, false, caches);
      TestingUtil.waitForRehashToComplete(caches);


      owners = getOwners(k);

      log.infof("After shutting one node down, owners list for key %s: %s", k, Arrays.asList(owners));

      assert owners.length == 2;

      for (Cache<Object, String> o : owners) {
         int numWrites = getCacheStoreStats(o, "store");
         assert numWrites == 1 : "store() should have been invoked on the cache store once.  Was " + numWrites;
         assert "v".equals(o.get(k)) : "Should be able to see key on new owner";
      }
   }
}
