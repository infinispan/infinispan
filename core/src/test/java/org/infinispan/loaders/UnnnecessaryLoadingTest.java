package org.infinispan.loaders;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * A test to ensure stuff from a cache store is not loaded unnecessarily if it already exists in memory.
 *
 * @author Manik Surtani
 * @version 4.1
 */
@Test(testName = "loaders.UnnnecessaryLoadingTest", groups = "functional")
public class UnnnecessaryLoadingTest extends SingleCacheManagerTest {
   CacheStore store;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      Configuration cfg = getDefaultStandaloneConfig(false);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.addCacheLoaderConfig(new CountingCacheStoreConfig());
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg());
      cfg.setCacheLoaderManagerConfig(clmc);
      CacheManager cm = TestCacheManagerFactory.createCacheManager(cfg, true);
      cache = cm.getCache();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      return cm;
   }

   public void testRepeatedLoads() throws CacheLoaderException {
      CacheLoaderManager clm = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      ChainingCacheStore ccs = (ChainingCacheStore) clm.getCacheLoader();
      CountingCacheStore countingCS = (CountingCacheStore) ccs.getStores().keySet().iterator().next();

      assert countingCS.numLoads == 0;
      assert countingCS.numContains == 0;
      store.store(InternalEntryFactory.create("k1", "v1"));

      assert countingCS.numLoads == 0;
      assert countingCS.numContains == 0;

      assert "v1".equals(cache.get("k1"));

      assert countingCS.numLoads == 1 : "Expected 1, was " + countingCS.numLoads;
      assert countingCS.numContains == 0 : "Expected 0, was " + countingCS.numContains;

      assert "v1".equals(cache.get("k1"));

      assert countingCS.numLoads == 1 : "Expected 1, was " + countingCS.numLoads;
      assert countingCS.numContains == 0 : "Expected 0, was " + countingCS.numContains;
   }

   public static class CountingCacheStore extends AbstractCacheStore {
      int numLoads, numContains;

      @Override
      public void store(InternalCacheEntry entry) throws CacheLoaderException {
      }

      @Override
      public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      }

      @Override
      public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      }

      @Override
      public void clear() throws CacheLoaderException {
      }

      @Override
      public boolean remove(Object key) throws CacheLoaderException {
         return false;
      }

      @Override
      protected void purgeInternal() throws CacheLoaderException {
      }

      @Override
      public InternalCacheEntry load(Object key) throws CacheLoaderException {
         numLoads++;
         return null;
      }

      @Override
      public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
         return Collections.emptySet();
      }

      @Override
      public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
         return Collections.emptySet();
      }

      @Override
      public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
         return Collections.emptySet();
      }

      @Override
      public boolean containsKey(Object key) throws CacheLoaderException {
         numContains++;
         return false;
      }

      @Override
      public Class<? extends CacheLoaderConfig> getConfigurationClass() {
         return CountingCacheStoreConfig.class;
      }
   }

   public static class CountingCacheStoreConfig extends AbstractCacheStoreConfig {
      public CountingCacheStoreConfig() {
         setCacheLoaderClassName(CountingCacheStore.class.getName());
      }
   }
}
