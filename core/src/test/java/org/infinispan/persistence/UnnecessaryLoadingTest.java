package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.concurrent.Executor;

import static org.infinispan.test.TestingUtil.marshaller;
import static org.testng.Assert.assertEquals;

/**
 * A test to ensure stuff from a cache store is not loaded unnecessarily if it already exists in memory, or if the
 * Flag.SKIP_CACHE_STORE is applied.
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @version 4.1
 */
@Test(testName = "persistence.UnnecessaryLoadingTest", groups = "functional", singleThreaded = true)
@CleanupAfterMethod
public class UnnecessaryLoadingTest extends SingleCacheManagerTest {
   AdvancedLoadWriteStore store;
   private PersistenceManagerImpl persistenceManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .invocationBatching().enable()
         .persistence()
            .addStore(CountingStoreConfigurationBuilder.class)
         .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      persistenceManager = (PersistenceManagerImpl) TestingUtil.extractComponent(cache, PersistenceManager.class);
      store = (AdvancedLoadWriteStore) persistenceManager.getAllWriters().get(1);

   }

   public void testRepeatedLoads() throws PersistenceException {
      CountingStore countingCS = getCountingCacheStore();
      store.write(new MarshalledEntryImpl("k1", "v1", null, marshaller(cache)));

      assert countingCS.numLoads == 0;
      assert countingCS.numContains == 0;

      assert "v1".equals(cache.get("k1"));

      assert countingCS.numLoads == 1 : "Expected 1, was " + countingCS.numLoads;
      assert countingCS.numContains == 0 : "Expected 0, was " + countingCS.numContains;

      assert "v1".equals(cache.get("k1"));

      assert countingCS.numLoads == 1 : "Expected 1, was " + countingCS.numLoads;
      assert countingCS.numContains == 0 : "Expected 0, was " + countingCS.numContains;
   }



   public void testSkipCacheFlagUsage() throws PersistenceException {
      CountingStore countingCS = getCountingCacheStore();

      store.write(new MarshalledEntryImpl("k1", "v1", null, marshaller(cache)));

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
      store.write(new MarshalledEntryImpl("k2", "v2", null, marshaller(cache)));
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
      assert "v2-second".equals(persistenceManager.loadFromAllStores("k2").getValue());
      assertEquals(countingCS.numLoads,2, "Expected 2, was " + countingCS.numLoads);

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

   private CountingStore getCountingCacheStore() {
      CountingStore countingCS = (CountingStore) TestingUtil.getFirstLoader(cache);
      reset(cache, countingCS);
      return countingCS;
   }

   public void testSkipCacheLoadFlagUsage() throws PersistenceException {
      CountingStore countingCS = getCountingCacheStore();

      TestObjectStreamMarshaller sm = new TestObjectStreamMarshaller();
      try {
         store.write(new MarshalledEntryImpl("home", "Vermezzo", null, sm));
         store.write(new MarshalledEntryImpl("home-second", "Newcastle Upon Tyne", null, sm));

         assert countingCS.numLoads == 0;
         //load using SKIP_CACHE_LOAD should not find the object in the store
         assert cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).get("home") == null;
         assert countingCS.numLoads == 0;

         assert cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).put("home", "Newcastle") == null;
         assert countingCS.numLoads == 0;

         final Object put = cache.getAdvancedCache().put("home-second", "Newcastle Upon Tyne, second");
         assertEquals(put, "Newcastle Upon Tyne");
         assert countingCS.numLoads == 1;
      } finally {
         sm.stop();
      }
   }

   private void reset(Cache<?, ?> cache, CountingStore countingCS) {
      cache.clear();
      countingCS.numLoads = 0;
      countingCS.numContains = 0;
   }

   public static class CountingStore implements AdvancedLoadWriteStore {
      public int numLoads, numContains;

      @Override
      public void process(KeyFilter filter, CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      }

      @Override
      public int size() {
         return 0;  
      }


      @Override
      public void clear() {
         
      }

      @Override
      public void purge(Executor threadPool, PurgeListener task) {
         
      }

      @Override
      public void init(InitializationContext ctx) {
         
      }

      @Override
      public void write(MarshalledEntry entry) {
         
      }

      @Override
      public boolean delete(Object key) {
         return false;  
      }


      @Override
      public MarshalledEntry load(Object key) throws PersistenceException {
         incrementLoads();
         return null;
      }

      @Override
      public void start() {
      }

      @Override
      public void stop() {
      }

      @Override
      public boolean contains(Object key) throws PersistenceException {
         numContains++;
         return false;
      }

      private void incrementLoads() {
         numLoads++;
      }
   }

   @BuiltBy(CountingStoreConfigurationBuilder.class)
   @ConfigurationFor(CountingStore.class)
   public static class CountingStoreConfiguration extends AbstractStoreConfiguration {

      public CountingStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties) {
         super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      }
   }

   public static class CountingStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<CountingStoreConfiguration, CountingStoreConfigurationBuilder> {

      public CountingStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder);
      }

      @Override
      public CountingStoreConfiguration create() {
         return new CountingStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(), singletonStore.create(), preload, shared, properties);
      }

      @Override
      public Builder<?> read(CountingStoreConfiguration template) {
         // AbstractStore-specific configuration
         fetchPersistentState = template.fetchPersistentState();
         ignoreModifications = template.ignoreModifications();
         properties = template.properties();
         purgeOnStartup = template.purgeOnStartup();
         async.read(template.async());
         singletonStore.read(template.singletonStore());
         return this;
      }

      @Override
      public CountingStoreConfigurationBuilder self() {
         return this;
      }

   }
}
