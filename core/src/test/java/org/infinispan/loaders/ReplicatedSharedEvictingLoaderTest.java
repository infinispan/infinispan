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

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * This test aims to ensure that a replicated cache with a shared loader, when using passivation and eviction, doesn't
 * remove entries from the cache store when activating.
 */
@Test(testName = "loaders.ReplicatedSharedEvictingLoaderTest", groups = "functional")
public class ReplicatedSharedEvictingLoaderTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg("ReplicatedSharedEvictingLoaderTest"));
      clmc.setShared(true);
      clmc.setPassivation(true);
      c.setCacheLoaderManagerConfig(clmc);
      createCluster(c, 2);
      waitForClusterToForm();
   }

   public void testRemovalFromCacheStoreOnEvict() {
      cache(0).put("k", "v");

      assert "v".equals(cache(0).get("k"));
      assert "v".equals(cache(1).get("k"));

      cache(0).evict("k");
      cache(1).evict("k");

      assert "v".equals(cache(0).get("k"));
      assert "v".equals(cache(1).get("k"));
   }
}
