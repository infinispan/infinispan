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
package org.infinispan.api.tree;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * @author <a href="mailto:konstantin.kuzmin@db.com">Konstantin Kuzmin</a>
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "api.tree.FlagTest")
public class FlagTest extends MultipleCacheManagersTest {
   private Cache<String, String> cache1, cache2;
   private TreeCache<String, String> treeCache1, treeCache2;
   private static final String KEY = "key";
   private static final Log log = LogFactory.getLog(FlagTest.class);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, true);
      cb.invocationBatching().enable();
      createClusteredCaches(2, "invalidatedFlagCache", cb);
      cache1 = cache(0, "invalidatedFlagCache");
      cache2 = cache(1, "invalidatedFlagCache");
      TreeCacheFactory tcf = new TreeCacheFactory();
      treeCache1 = tcf.createTreeCache(cache1);
      treeCache2 = tcf.createTreeCache(cache2);
   }

   public void testTreeCacheLocalPut() throws Exception {
      final Fqn fqn = Fqn.fromElements("TEST");
      treeCache1.put(fqn, KEY, "1", Flag.CACHE_MODE_LOCAL);
      treeCache2.put(fqn, KEY, "2", Flag.CACHE_MODE_LOCAL);
      assert "2".equals(treeCache2.get(fqn, KEY)) : "treeCache2 was updated locally";
      assert "1".equals(treeCache1.get(fqn, KEY)) : "treeCache1 should not be invalidated in case of LOCAL put in treeCache2";

      String fqnString = "fqnAsString";
      treeCache1.put(fqnString, KEY, "3", Flag.CACHE_MODE_LOCAL);
      treeCache2.put(fqnString, KEY, "4", Flag.CACHE_MODE_LOCAL);
      assert "4".equals(treeCache2.get(fqnString, KEY)) : "treeCache2 was updated locally";
      assert "3".equals(treeCache1.get(fqnString, KEY)) : "treeCache1 should not be invalidated in case of LOCAL put in treeCache2";
   }

   public void testWithFlags() {
      AdvancedCache<String, String> localCache1 = cache1.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
      AdvancedCache<String, String> localCache2 = cache2.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
      TreeCache<String, String> treeCache1 = new TreeCacheFactory().createTreeCache(localCache1);
      TreeCache<String, String> treeCache2 = new TreeCacheFactory().createTreeCache(localCache2);
      final Fqn fqn = Fqn.fromElements("TEST_WITH_FLAGS");
      treeCache1.put(fqn, KEY, "1");
      treeCache2.put(fqn, KEY, "2");
      assert "2".equals(treeCache2.get(fqn, KEY)) : "treeCache2 was updated locally";
      assert "1".equals(treeCache1.get(fqn, KEY)) : "treeCache1 should not be invalidated in case of LOCAL put in treeCache2";
   }
}
