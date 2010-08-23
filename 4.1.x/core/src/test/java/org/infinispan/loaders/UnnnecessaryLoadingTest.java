package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * A test to ensure stuff from a cache store is not loaded unnecessarily if it already exists in memory,
 * or if the Flag.SKIP_CACHE_STORE is applied.
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @version 4.1
 */
@Test(testName = "loaders.UnnnecessaryLoadingTest", groups = "functional")
public class UnnnecessaryLoadingTest extends SingleCacheManagerTest {
   CacheStore store;
   
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration cfg = getDefaultStandaloneConfig(false);
      cfg.setInvocationBatchingEnabled(true);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.addCacheLoaderConfig(new CountingCacheStoreConfig());
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg());
      cfg.setCacheLoaderManagerConfig(clmc);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cfg, true);
      cache = cm.getCache();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      return cm;
   }

   public void testRepeatedLoads() throws CacheLoaderException {
      CacheLoaderManager clm = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      ChainingCacheStore ccs = (ChainingCacheStore) clm.getCacheLoader();
      CountingCacheStore countingCS = (CountingCacheStore) ccs.getStores().keySet().iterator().next();
      reset(cache,countingCS);
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

   public void testSkipCacheFlagUsage() throws CacheLoaderException {
      CacheLoaderManager clm = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      ChainingCacheStore ccs = (ChainingCacheStore) clm.getCacheLoader();
      CountingCacheStore countingCS = (CountingCacheStore) ccs.getStores().keySet().iterator().next();
      reset(cache, countingCS);
      
      store.store(InternalEntryFactory.create("k1", "v1"));

      assert countingCS.numLoads == 0;
      assert countingCS.numContains == 0;
      //load using SKIP_CACHE_STORE should not find the object in the store
      assert cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).get("k1") == null;
      assert countingCS.numLoads == 0;
      assert countingCS.numContains == 0;

      // counter-verify that the object was actually in the store:
      assert "v1".equals(cache.get("k1"));
      assert countingCS.numLoads == 1 : "Expected 1, was " + countingCS.numLoads;
      assert countingCS.numContains == 0 : "Expected 0, was " + countingCS.numContains;
      
      // now check that put won't return the stored value
      store.store(InternalEntryFactory.create("k2", "v2"));
      Object putReturn = cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).put("k2", "v2-second");
      assert putReturn == null;
      assert countingCS.numLoads == 1 : "Expected 1, was " + countingCS.numLoads;
      assert countingCS.numContains == 0 : "Expected 0, was " + countingCS.numContains;
      // but it inserted it in the cache:
      assert "v2-second".equals(cache.get("k2"));
      // perform the put in the cache & store, using same value:
      putReturn = cache.put("k2", "v2-second");
      //returned value from the cache:
      assert "v2-second".equals(putReturn);
      //and verify that the put operation updated the store too:
      assert "v2-second".equals(store.load("k2").getValue());
      assert countingCS.numLoads == 2 : "Expected 2, was " + countingCS.numLoads;
      
      assert countingCS.numContains == 0 : "Expected 0, was " + countingCS.numContains;
      cache.containsKey("k1");
      assert countingCS.numContains == 0 : "Expected 0, was " + countingCS.numContains;
      assert false == cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).containsKey("k3");
      assert countingCS.numContains == 0 : "Expected 0, was " + countingCS.numContains;
      assert countingCS.numLoads == 2 : "Expected 2, was " + countingCS.numLoads;
      
      //now with batching:
      boolean batchStarted = cache.getAdvancedCache().startBatch();
      assert batchStarted;
      assert null == cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).get("k1batch");
      assert countingCS.numLoads == 2 : "Expected 2, was " + countingCS.numLoads;
      assert null == cache.getAdvancedCache().get("k2batch");
      assert countingCS.numLoads == 3 : "Expected 2, was " + countingCS.numLoads;
      cache.endBatch(true);
   }
   
   private void reset(Cache cache, CountingCacheStore countingCS) {
      cache.clear();
      countingCS.numLoads = 0;
      countingCS.numContains = 0;
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
