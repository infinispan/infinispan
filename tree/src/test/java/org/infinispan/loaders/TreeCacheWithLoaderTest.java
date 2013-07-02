package org.infinispan.loaders;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LegacyStoreConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
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
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(true);
      cb.invocationBatching().enable();
      addCacheStore(cb.loaders().addStore());
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cb);
      cache = new TreeCacheImpl<String, String>(cm.getCache());
      store = extractCacheStore();
      return cm;
   }

   protected void addCacheStore(LegacyStoreConfigurationBuilder cb) {
      cb.cacheStore(new DummyInMemoryCacheStore())
            .addProperty("storeName", getClass().getName());
   }

   public void testPersistence() throws CacheLoaderException {
      cache.put("/a/b/c", "key", "value");
      assert "value".equals(cache.get("/a/b/c", "key"));

      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), DATA));
      assert "value".equals(nodeContentsInCacheStore(store, Fqn.fromString("/a/b/c")).get("key"));
      assert store.containsKey(new NodeKey(Fqn.fromString("/a/b/c"), STRUCTURE));

      restartCache();
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

      restartCache();
      assert "value".equals(cache.get(ROOT, "key"));

      assert store.containsKey(new NodeKey(ROOT, DATA));
      assert "value".equals(nodeContentsInCacheStore(store, ROOT).get("key"));
      assert store.containsKey(new NodeKey(ROOT, STRUCTURE));
   }

   public void testDuplicatePersistence() throws CacheLoaderException {
      cache.put(Fqn.fromElements("a", "b"), "k", "v");
      assert "v".equals(cache.get(Fqn.fromElements("a", "b"), "k"));
      restartCache();
      cache.put(Fqn.fromElements("a", "b"), "k", "v");
      assert "v".equals(cache.get(Fqn.fromElements("a", "b"), "k"));
   }

   @SuppressWarnings("unchecked")
   private Map<String, String> nodeContentsInCacheStore(CacheStore cs, Fqn fqn) throws CacheLoaderException {
      return (Map<String, String>) cs.load(new NodeKey(fqn, DATA)).getValue();
   }

   private CacheStore extractCacheStore() {
      return cache.getCache().getAdvancedCache().getComponentRegistry()
            .getComponent(CacheLoaderManager.class).getCacheStore();
   }

   private void restartCache() {
      cache.stop();
      cache.start();
      store = extractCacheStore();
   }

}
