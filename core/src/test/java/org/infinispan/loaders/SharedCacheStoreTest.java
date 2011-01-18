package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
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
      Configuration cfg = new Configuration();
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.setShared(true);
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg(false));
      cfg.setCacheLoaderManagerConfig(clmc);
      cfg.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      createCluster(cfg, true, 3);
   }

   private List<CacheStore> cachestores() {
      List<CacheStore> l = new LinkedList<CacheStore>();
      for (Cache<?, ?> c: caches())
         l.add(TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore());
      return l;
   }

   public void testUnnecessaryWrites() throws CacheLoaderException {
      cache(0).put("key", "value");

      for (Cache<Object, Object> c: caches())
         assert "value".equals(c.get("key"));

      for (CacheStore cs: cachestores()) {
         assert cs.containsKey("key");
         DummyInMemoryCacheStore dimcs = (DummyInMemoryCacheStore) cs;
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
