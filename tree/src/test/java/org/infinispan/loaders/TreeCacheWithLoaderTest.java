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

   @SuppressWarnings("unchecked")
   private Map<String, String> nodeContentsInCacheStore(CacheStore cs, Fqn fqn) throws CacheLoaderException {
      return (Map<String, String>) cs.load(new NodeKey(fqn, DATA)).getValue();
   }

}
