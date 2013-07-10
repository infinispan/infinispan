package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.AbstractCacheStore;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * A test to ensure stuff from a cache store is not loaded unnecessarily if it already exists in memory, or if the
 * Flag.SKIP_CACHE_STORE is applied.
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @version 4.1
 */
@Test(testName = "loaders.UnnnecessaryLoadingTest", groups = "functional", singleThreaded = true)
@CleanupAfterMethod
public class UnnnecessaryLoadingTest extends SingleCacheManagerTest {
   CacheStore store;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .invocationBatching().enable()
         .loaders()
            .addStore(CountingCacheStoreConfigurationBuilder.class)
         .loaders()
            .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
   }

   public void testRepeatedLoads() throws CacheLoaderException {
      CountingCacheStore countingCS = getCountingCacheStore();
      store.store(TestInternalCacheEntryFactory.create("k1", "v1"));

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
      CountingCacheStore countingCS = getCountingCacheStore();

      store.store(TestInternalCacheEntryFactory.create("k1", "v1"));

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
      store.store(TestInternalCacheEntryFactory.create("k2", "v2"));
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
      assert countingCS.numLoads == 3 : "Expected 3, was " + countingCS.numLoads;
      cache.endBatch(true);
   }

   private CountingCacheStore getCountingCacheStore() {
      CacheLoaderManager clm = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      ChainingCacheStore ccs = (ChainingCacheStore) clm.getCacheLoader();
      CountingCacheStore countingCS = (CountingCacheStore) ccs.getStores().keySet().iterator().next();
      reset(cache, countingCS);
      return countingCS;
   }

   public void testSkipCacheLoadFlagUsage() throws CacheLoaderException {
      CountingCacheStore countingCS = getCountingCacheStore();

      store.store(TestInternalCacheEntryFactory.create("home", "Vermezzo"));
      store.store(TestInternalCacheEntryFactory.create("home-second", "Newcastle Upon Tyne"));

      assert countingCS.numLoads == 0;
      //load using SKIP_CACHE_LOAD should not find the object in the store
      assert cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).get("home") == null;
      assert countingCS.numLoads == 0;

      assert cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).put("home", "Newcastle") == null;
      assert countingCS.numLoads == 0;

      final Object put = cache.getAdvancedCache().put("home-second", "Newcastle Upon Tyne, second");
      assertEquals(put, "Newcastle Upon Tyne");
      assert countingCS.numLoads == 1;
   }

   private void reset(Cache<?, ?> cache, CountingCacheStore countingCS) {
      cache.clear();
      countingCS.numLoads = 0;
      countingCS.numContains = 0;
   }

   public static class CountingCacheStore extends AbstractCacheStore  {
      public int numLoads, numContains;

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
         incrementLoads();
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

      private void incrementLoads() {
         numLoads++;
      }
   }

   @BuiltBy(CountingCacheStoreConfigurationBuilder.class)
   @ConfigurationFor(CountingCacheStore.class)
   public static class CountingCacheStoreConfiguration extends AbstractStoreConfiguration {
      protected CountingCacheStoreConfiguration(boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications,
            TypedProperties properties, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
         super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties, async, singletonStore);
      }

   }

   public static class CountingCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<CountingCacheStoreConfiguration, CountingCacheStoreConfigurationBuilder> {

      public CountingCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
         super(builder);
      }

      @Override
      public CountingCacheStoreConfiguration create() {
         return new CountingCacheStoreConfiguration(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
      }

      @Override
      public Builder<?> read(CountingCacheStoreConfiguration template) {
         // AbstractStore-specific configuration
         fetchPersistentState = template.fetchPersistentState();
         ignoreModifications = template.ignoreModifications();
         properties = template.properties();
         purgeOnStartup = template.purgeOnStartup();
         purgeSynchronously = template.purgeSynchronously();
         async.read(template.async());
         singletonStore.read(template.singletonStore());
         return this;
      }

      @Override
      public CountingCacheStoreConfigurationBuilder self() {
         return this;
      }

   }
}
