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
package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

@Test (testName = "loaders.SharedCacheStoreTest", groups = "functional")
public class SharedCacheStoreTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = new Configuration().fluent()
         .loaders()
            .shared(true)
            .addCacheLoader(new DummyInMemoryCacheStore.Cfg()
               .storeName(SharedCacheStoreTest.class.getName())
               .purgeOnStartup(false))
         .clustering()
            .mode(Configuration.CacheMode.REPL_SYNC)
         .build();
      createCluster(cfg, 3);
   }

   private List<CacheStore> cachestores() {
      List<CacheStore> l = new LinkedList<CacheStore>();
      for (Cache<?, ?> c: caches())
         l.add(TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore());
      return l;
   }

   public void testUnnecessaryWrites() throws CacheLoaderException {
      cache(0).put("key", "value");

      // the second and third cache are only started here
      // so state transfer will copy the key to the other caches
      // however is should not write it to the cache store again
      for (Cache<Object, Object> c: caches())
         assert "value".equals(c.get("key"));

      for (CacheStore cs: cachestores()) {
         assert cs.containsKey("key");
         DummyInMemoryCacheStore dimcs = (DummyInMemoryCacheStore) cs;
         assert dimcs.stats().get("clear") == 0: "Cache store should not be cleared, purgeOnStartup is false";
         assert dimcs.stats().get("store") == 1: "Cache store should have been written to just once, but was written to " + dimcs.stats().get("store") + " times";
      }

      cache(0).remove("key");

      for (Cache<Object, Object> c: caches())
         assert c.get("key") == null;

      for (CacheStore cs: cachestores()) {
         assert !cs.containsKey("key");
         DummyInMemoryCacheStore dimcs = (DummyInMemoryCacheStore) cs;
         assert dimcs.stats().get("remove") == 1: "Entry should have been removed from the cache store just once, but was removed " + dimcs.stats().get("store") + " times";
      }
   }

}
