package org.infinispan.loaders;

import org.infinispan.atomic.AtomicMap;
import org.infinispan.config.CacheLoaderManagerConfig;
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

@Test(groups = "functional", testName = "loaders.TreeCacheWithLoaderTest")
public class TreeCacheWithLoaderTest extends SingleCacheManagerTest {

   TreeCache<String, String> cache;
   CacheStore store;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      Configuration c = getDefaultStandaloneConfig(true);
      c.setInvocationBatchingEnabled(true);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg());
      c.setCacheLoaderManagerConfig(clmc);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c, true);
      cache = new TreeCacheImpl<String, String>(cm.getCache());
      CacheLoaderManager m = TestingUtil.extractComponent(cache.getCache(), CacheLoaderManager.class);
      store = m.getCacheStore();
      return cm;
   }

   public void testPersistence() throws CacheLoaderException {
      cache.put("/a/b/c", "key", "value");
      assert "value".equals(cache.get("/a/b/c", "key"));

      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), NodeKey.Type.DATA));
      assert "value".equals(((AtomicMap) store.load(new NodeKey(Fqn.fromString("/a/b/c"), NodeKey.Type.DATA)).getValue()).get("key"));
      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), NodeKey.Type.STRUCTURE));

      cache.stop();
      cache.start();
      assert "value".equals(cache.get("/a/b/c", "key"));
      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), NodeKey.Type.DATA));
      assert "value".equals(((AtomicMap) store.load(new NodeKey(Fqn.fromString("/a/b/c"), NodeKey.Type.DATA)).getValue()).get("key"));
      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), NodeKey.Type.STRUCTURE));
   }

   public void testRootNodePersistence() throws CacheLoaderException {
      cache.put(Fqn.ROOT, "key", "value");
      assert "value".equals(cache.get(Fqn.ROOT, "key"));
      assert store.containsKey(new NodeKey(Fqn.ROOT, NodeKey.Type.DATA));
      assert "value".equals(((AtomicMap) store.load(new NodeKey(Fqn.ROOT, NodeKey.Type.DATA)).getValue()).get("key"));
      assert store.containsKey(new NodeKey(Fqn.ROOT, NodeKey.Type.STRUCTURE));

      cache.stop();
      cache.start();
      assert "value".equals(cache.get(Fqn.ROOT, "key"));

      assert store.containsKey(new NodeKey(Fqn.ROOT, NodeKey.Type.DATA));
      assert "value".equals(((AtomicMap) store.load(new NodeKey(Fqn.ROOT, NodeKey.Type.DATA)).getValue()).get("key"));
      assert store.containsKey(new NodeKey(Fqn.ROOT, NodeKey.Type.STRUCTURE));
   }

}
