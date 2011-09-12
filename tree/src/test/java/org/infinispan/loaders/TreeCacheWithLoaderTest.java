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
package org.infinispan.loaders;

import org.infinispan.config.Configuration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.NodeKey;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheImpl;
import org.testng.annotations.Test;

import java.util.Map;

import static org.infinispan.tree.Fqn.ROOT;
import static org.infinispan.tree.NodeKey.Type.DATA;
import static org.infinispan.tree.NodeKey.Type.STRUCTURE;

@Test(groups = "functional", testName = "loaders.TreeCacheWithLoaderTest")
public class TreeCacheWithLoaderTest extends SingleCacheManagerTest {

   TreeCache<String, String> cache;
   CacheStore store;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      Configuration c = getDefaultStandaloneConfig(true).fluent()
            .invocationBatching()
            .loaders()
               .addCacheLoader(new DummyInMemoryCacheStore.Cfg(getClass().getName()))
            .build();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c);
      cache = new TreeCacheImpl<String, String>(cm.getCache());
      CacheLoaderManager m = TestingUtil.extractComponent(cache.getCache(), CacheLoaderManager.class);
      store = m.getCacheStore();
      return cm;
   }

   public void testPersistence() throws CacheLoaderException {
      cache.put("/a/b/c", "key", "value");
      assert "value".equals(cache.get("/a/b/c", "key"));

      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), DATA));
      assert "value".equals(nodeContentsInCacheStore(store, Fqn.fromString("/a/b/c")).get("key"));
      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), STRUCTURE));

      cache.stop();
      cache.start();
      assert "value".equals(cache.get("/a/b/c", "key"));
      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), DATA));
      assert "value".equals(nodeContentsInCacheStore(store, Fqn.fromString("/a/b/c")).get("key"));
      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), STRUCTURE));
   }

   public void testRootNodePersistence() throws CacheLoaderException {
      cache.put(ROOT, "key", "value");
      assert "value".equals(cache.get(ROOT, "key"));
      assert store.containsKey(new NodeKey(ROOT, DATA));
      assert "value".equals(nodeContentsInCacheStore(store, ROOT).get("key"));
      assert store.containsKey(new NodeKey(ROOT, STRUCTURE));

      cache.stop();
      cache.start();
      assert "value".equals(cache.get(ROOT, "key"));

      assert store.containsKey(new NodeKey(ROOT, DATA));
      assert "value".equals(nodeContentsInCacheStore(store, ROOT).get("key"));
      assert store.containsKey(new NodeKey(ROOT, STRUCTURE));
   }

   public void testDuplicatePersistence() throws CacheLoaderException {
      cache.put(Fqn.fromElements("a", "b"), "k", "v");
      assert "v".equals(cache.get(Fqn.fromElements("a", "b"), "k"));
      cache.stop();
      cache.start();
      cache.put(Fqn.fromElements("a", "b"), "k", "v");
      assert "v".equals(cache.get(Fqn.fromElements("a", "b"), "k"));
   }

   @SuppressWarnings("unchecked")
   private Map<String, String> nodeContentsInCacheStore(CacheStore cs, Fqn fqn) throws CacheLoaderException {
      return (Map<String, String>) cs.load(new NodeKey(fqn, DATA)).getValue();
   }

}
